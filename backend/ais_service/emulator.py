
import asyncio
import json
import math
import random
import time
import websockets

ROUTE_AKTAU_TO_KASHAGAN = [
    {"lat": 43.6529, "lon": 51.1694, "name": "Порт Актау"},
    {"lat": 43.80,   "lon": 50.10,   "name": "Открытое море, запад от Актау"},
    {"lat": 44.70,   "lon": 49.85,   "name": "Открытое море, транзит на север"},
    {"lat": 45.60,   "lon": 50.30,   "name": "Северный Каспий, открытая вода"},
    {"lat": 46.05,   "lon": 51.40,   "name": "Поворот на восток, севернее п-ва Бузачи"},
    {"lat": 45.90,   "lon": 52.50,   "name": "Лежбища тюленя (о-ва Дурнева/Прорва/Ремонтные Шалыги)"},
    {"lat": 46.20,   "lon": 52.00,   "name": "Подход к Кашагану"},
    {"lat": 46.50,   "lon": 51.90,   "name": "Месторождение Кашаган (~80 км от Атырау)"},
]

PROTECTED_ZONE = {"lat": 45.9000, "lon": 52.5250, "radius_km": 35}

OILFIELD_ZONE = {"lat": 46.5000, "lon": 51.9000, "radius_km": 15}

KNOTS_TO_KMH = 1.852
TICK_SECONDS = 2                  

SIMULATION_SPEED_MULTIPLIER = 150


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def bearing_deg(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    x = math.sin(dlambda) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dlambda)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def interpolate(p1, p2, frac):
    lat = p1["lat"] + (p2["lat"] - p1["lat"]) * frac
    lon = p1["lon"] + (p2["lon"] - p1["lon"]) * frac
    return lat, lon


def in_zone(lat, lon, zone):
    return haversine_km(lat, lon, zone["lat"], zone["lon"]) <= zone["radius_km"]


class Vessel:
    def __init__(self, mmsi: int, name: str, speed_knots: float, suspicious: bool = False):
        self.mmsi = mmsi
        self.name = name
        self.speed_knots = speed_knots
        self.suspicious = suspicious

        self.lat = 0.0
        self.lon = 0.0
        self.course = 0.0
        self.last_seen = time.time()
        self.ais_online = True

        self.segment_idx = 0
        self.segment_frac = 0.0
        self.dark_ticks_left = 0

    def update_position_from_simulation(self):
        p1 = ROUTE_AKTAU_TO_KASHAGAN[self.segment_idx]
        p2_idx = min(self.segment_idx + 1, len(ROUTE_AKTAU_TO_KASHAGAN) - 1)
        p2 = ROUTE_AKTAU_TO_KASHAGAN[p2_idx]

        segment_km = haversine_km(p1["lat"], p1["lon"], p2["lat"], p2["lon"])
        speed_kmh = self.speed_knots * KNOTS_TO_KMH
        step_km = speed_kmh * (TICK_SECONDS / 3600) * SIMULATION_SPEED_MULTIPLIER

        if segment_km > 0:
            self.segment_frac += step_km / segment_km

        while self.segment_frac >= 1.0 and self.segment_idx < len(ROUTE_AKTAU_TO_KASHAGAN) - 2:
            self.segment_frac -= 1.0
            self.segment_idx += 1

        if self.segment_idx >= len(ROUTE_AKTAU_TO_KASHAGAN) - 2 and self.segment_frac >= 1.0:
            self.segment_idx = 0
            self.segment_frac = 0.0

        self.lat, self.lon = interpolate(p1, p2, self.segment_frac)
        self.course = bearing_deg(p1["lat"], p1["lon"], p2["lat"], p2["lon"])

    def update_ais_status(self):
        if not self.suspicious:
            self.ais_online = True
            self.last_seen = time.time()
            return

        if self.dark_ticks_left > 0:
            self.dark_ticks_left -= 1
            self.ais_online = False
            return

        if in_zone(self.lat, self.lon, PROTECTED_ZONE):
            if random.random() < 0.15:
                self.dark_ticks_left = random.randint(6, 12)
                self.ais_online = False
                return

        self.ais_online = True
        self.last_seen = time.time()

    def to_packet(self, data_source: str = "SIMULATED_S-AIS"):
        self.update_position_from_simulation()
        self.update_ais_status()

        return {
            "mmsi": self.mmsi,
            "vessel_name": self.name,
            "source": data_source,
            "latitude": round(self.lat, 5),
            "longitude": round(self.lon, 5),
            "speed_knots": round(self.speed_knots + random.uniform(-0.3, 0.3), 1),
            "course": round(self.course, 1),
            "ais_online": self.ais_online,
            "timestamp": int(self.last_seen),
            "zones": {
                "in_protected_zone": in_zone(self.lat, self.lon, PROTECTED_ZONE),
                "in_oilfield_zone": in_zone(self.lat, self.lon, OILFIELD_ZONE),
            }
        }


VESSELS = [
    Vessel(mmsi=436123456, name="Caspian Tanker 01", speed_knots=11.5, suspicious=False),
    Vessel(mmsi=436987654, name="Unknown Vessel 02", speed_knots=13.0, suspicious=True),
]


async def generate_ais_telemetry(websocket):
    print("Клиент успешно подключился к WebSocket-потоку AIS")
    try:
        while True:
            packets = []
            for vessel in VESSELS:
                packet = vessel.to_packet()
                packets.append(packet)

                status = "АКТИВЕН" if packet["ais_online"] else "DARK SHIP (СИГНАЛ ПОТЕРЯН)"
                zone_flag = " [ЗАПОВЕДНИК]" if packet["zones"]["in_protected_zone"] else ""
                print(f"📡 [{packet['vessel_name']}] Lat: {packet['latitude']}, Lon: {packet['longitude']} "
                      f"| AIS: {status}{zone_flag}")

            await websocket.send(json.dumps({
                "event": "ais_telemetry_update",
                "vessels": packets
            }))
            await asyncio.sleep(TICK_SECONDS)

    except websockets.exceptions.ConnectionClosed:
        print("Клиент отключился от WebSocket-потока.")


async def main():
    async with websockets.serve(generate_ais_telemetry, "localhost", 8765):
        print("AIS Telemetry Server запущен и ожидает подключений на ws://localhost:8765")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())