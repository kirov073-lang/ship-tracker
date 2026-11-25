"""Local fake of the aisstream websocket, for testing collector.py offline."""
import asyncio, json, random, sys
import websockets

SHIPS = [  # mmsi, imo, name, call, type, len, beam, lat, lon, dest
 (636021920, 9854753, "MOHICAN", "D5ZZ9", 70, 200, 32, 46.10, 30.95, "UA CHORNOMORSK"),
 (244730000, 9296547, "RLO TRADER", "PBAA", 80, 144, 23, 51.95, 4.05, "NL RTM"),
 (538007777, 9260079, "PIONEER", "V7AB1", 80, 183, 32, 1.20, 103.80, "SG SIN"),
 (636019999, 9811000, "MSC ARINA", "D5XX1", 71, 399, 61, 36.10, 14.40, "EG PSD"),
 (215000111, 9700001, "SEA HAWK", "9HAB", 70, 190, 32, 41.00, 29.00, "TR IST"),
]

async def handler(ws):
    sub = json.loads(await ws.recv())
    assert sub["APIKey"] == "test-key", sub
    for i in range(400):
        s = random.choice(SHIPS)
        if i % 7 == 0:
            name = s[2] if not (s[2] == "SEA HAWK" and i > 200) else "SEA EAGLE"
            msg = {"MessageType": "ShipStaticData", "MetaData": {"MMSI": s[0], "ShipName": name},
                   "Message": {"ShipStaticData": {"UserID": s[0], "ImoNumber": s[1], "Name": name + "@@@@",
                   "CallSign": s[3], "Type": s[4], "Dimension": {"A": s[5]-20, "B": 20, "C": s[6]//2, "D": s[6]-s[6]//2},
                   "Destination": s[9], "Eta": {"Month": 11, "Day": 28, "Hour": 6, "Minute": 0}, "MaximumStaticDraught": 11.2}}}
        else:
            msg = {"MessageType": "PositionReport", "MetaData": {"MMSI": s[0], "ShipName": s[2]},
                   "Message": {"PositionReport": {"UserID": s[0], "Latitude": s[7]+random.random()*0.2,
                   "Longitude": s[8]+random.random()*0.2, "Sog": 12.3, "Cog": 245.0, "TrueHeading": 244, "NavigationalStatus": 0}}}
        # a fishing boat without IMO must be ignored
        if i == 3:
            msg = {"MessageType": "ShipStaticData", "MetaData": {"MMSI": 272111222},
                   "Message": {"ShipStaticData": {"UserID": 272111222, "ImoNumber": 0, "Name": "RYBAK", "Type": 30}}}
        await ws.send(json.dumps(msg))
        await asyncio.sleep(0.005)
    await asyncio.sleep(30)

async def main():
    async with websockets.serve(handler, "127.0.0.1", 8765):
        await asyncio.sleep(float(sys.argv[1]) if len(sys.argv) > 1 else 20)

asyncio.run(main())
