import asyncio
from bleak import BleakScanner, BleakClient
import time
import signal
from logging import info, debug
import logging
import threading
logging.basicConfig(level=logging.INFO)


# zero means parse all incoming data (every second)
CELL_INFO_REFRESH_S = 0
CHAR_HANDLE="0000ffe1-0000-1000-8000-00805f9b34fb"
MODEL_NBR_UUID = "00002a24-0000-1000-8000-00805f9b34fb"

COMMAND_CELL_INFO = 0x96;
COMMAND_DEVICE_INFO = 0x97;

FRAME_VERSION_JK04 = 0x01
FRAME_VERSION_JK02 = 0x02
FRAME_VERSION_JK02_32S = 0x03
PROTOCOL_VERSION_JK02 = 0x02


protocol_version=PROTOCOL_VERSION_JK02


MIN_RESPONSE_SIZE = 300;
MAX_RESPONSE_SIZE = 320;

class JkBmsBle:
    frame_buffer = bytearray()
    bms_status = {}

    waiting_for_response=""
    last_cell_info=0

    async def scanForDevices(self):
        devices = await BleakScanner.discover()
        for d in devices:
            print(d)

    def ba_to_int(self, arr, inclStart, byteCount):
        return int.from_bytes(arr[inclStart:inclStart+byteCount],byteorder="little")

    def decode_cellinfo_jk02(self):
       # global bms_status
        fb=self.frame_buffer
        for i in range(0,self.bms_status["cell_count"]):
            self.bms_status["cell_"+str(i+1)+"_voltage"]=self.ba_to_int(fb,6+(2*i),2)*0.001

        debug(self.bms_status)

    def decode_settings_jk02(self):
       # global bms_status
        fb=self.frame_buffer
    
        self.bms_status["cell_uvp"]=self.ba_to_int(fb,10,4)*0.001
        self.bms_status["cell_uvpr"]=self.ba_to_int(fb,14,4)*0.001
        self.bms_status["cell_ovp"]=self.ba_to_int(fb,18,4)*0.001

        self.bms_status["cell_ovpr"]=self.ba_to_int(fb,22,4)*0.001
    
        self.bms_status["balance_trigger_voltage"]=self.ba_to_int(fb,26,4)*0.001
        self.bms_status["power_off_voltage"]=self.ba_to_int(fb,46,4)*0.001
        self.bms_status["max_charge_current"]=self.ba_to_int(fb,50,4)*0.001


        self.bms_status["cell_count"]=self.ba_to_int(fb,114,4)
       # bms_status["charging_allowed"]=False if ba_to_int(fb,118,4) == 0 else True
       # bms_status["discharging_allowed"]=False if ba_to_int(fb,122,4) == 0 else True
    
        debug(self.bms_status)

    def decode(self):
        #global last_cell_info, waiting_for_response
        #check what kind of info the frame contains
        info_type=self.frame_buffer[4]
        if info_type == 0x01:
            info("Processing frame with settings info")
            if protocol_version == PROTOCOL_VERSION_JK02:
                self.decode_settings_jk02()
            else:
                return
        elif info_type == 0x02:
            if CELL_INFO_REFRESH_S==0 or time.time()-last_cell_info > CELL_INFO_REFRESH_S:
                self.last_cell_info=time.time()
                info("processing frame with battery cell info")
                if protocol_version == PROTOCOL_VERSION_JK02:
                    self.decode_cellinfo_jk02()
                else:
                    return
                if self.waiting_for_response=="cell_info":
                    self.waiting_for_response=""

        elif info_type == 0x03:
            if self.waiting_for_response=="device_info":
                self.waiting_for_response=""
            info("processing frame with device info")

    def assemble_frame(self, data:bytearray):
        #global frame_buffer
        if len(self.frame_buffer) > MAX_RESPONSE_SIZE:
            info("data dropped because it alone was longer than max frame length")
            self.frame_buffer=[]
    
        if data[0] == 0x55 and data[1] == 0xAA and data[2] == 0xEB and data[3] == 0x90:
            #beginning of new frame, clear buffer
            self.frame_buffer=[]
    
        self.frame_buffer.extend(data)
    
        #print(data)

        if len(self.frame_buffer) >= MIN_RESPONSE_SIZE:
            #check crc; always at position 300, independent of actual frame-lentgh, so crc up to 299
            ccrc=self.crc(self.frame_buffer,300-1)
            rcrc=self.frame_buffer[300-1]
            debug(f"compair recvd. crc: {rcrc} vs calc. crc: {ccrc}") 
            if ccrc == rcrc:
                info("great success! frame complete and sane, lets go decoding")
                self.decode()
                self.frame_buffer=[]

    def ncallback(self, sender: int, data: bytearray):
        debug(f"------> NEW PACKAGE!laenge:  {len(data)}")
        self.assemble_frame(data)

    def crc(self, arr:bytearray, length: int) -> int:
        crc = 0;
        for a in arr[:length]:
            crc = crc + a;
        return crc.to_bytes(2, 'little')[0]

    async def writeRegister(self, address, vals :bytearray,length:int, bleakC:BleakClient):
        frame = bytearray(20)
        frame[0] = 0xAA      #start sequence
        frame[1] = 0x55      #start sequence
        frame[2] = 0x90      #start sequence
        frame[3] = 0xEB      #start sequence
        frame[4] = address   #holding register
        frame[5] = length    #size of the value in byte
        frame[6] = vals[0]
        frame[7] = vals[1]
        frame[8] = vals[2]
        frame[9] = vals[3]
        frame[10] = 0x00
        frame[11] = 0x00
        frame[12] = 0x00
        frame[13] = 0x00
        frame[14] = 0x00
        frame[15] = 0x00
        frame[16] = 0x00
        frame[17] = 0x00
        frame[18] = 0x00
    
        frame[19] = self.crc(frame,len(frame)-1)
        debug( "Write register: ",frame )
        await bleakC.write_gatt_char(CHAR_HANDLE,frame, False)



    async def request_bt(self, rtype :str, client):   
        #global waiting_for_response


        timeout = time.time()

        while self.waiting_for_response!="" and time.time()-timeout < 10 :
            await asyncio.sleep(1)
            print(self.waiting_for_response)


        if rtype=="cell_info":
            cmd=COMMAND_CELL_INFO
            self.waiting_for_response="cell_info"
        elif rtype=="device_info":
            cmd=COMMAND_DEVICE_INFO
            self.waiting_for_response="device_info"
        else:
            return
            
        await self.writeRegister(cmd,b'\0\0\0\0',0x00,client)
    
    def get_status(self):
        return self.bms_status

 
    def __init__(self, addr):
        self.address = addr

    def connect_and_scrape(self):
        asyncio.run(self.asy_connect_and_scrape())
    
    async def asy_connect_and_scrape(self):
        print("connect and scrape on address"+self.address)
        self.run = True
        while self.run: #autoreconnect
            BleakClient(self.address)
            print("btloop")
            async with BleakClient(self.address) as client:
                self.bms_status["model_nbr"]=  (await client.read_gatt_char(MODEL_NBR_UUID)).decode("utf-8")
    
                await client.start_notify(CHAR_HANDLE, self.ncallback)
#
                await self.request_bt("device_info", client)
                await self.request_bt("cell_info", client)
                while client.is_connected and self.run:       
                    await asyncio.sleep(0.01)
        
        print("Exiting bt-loop")

    
    def start_scraping(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        bt_thread = threading.Thread(target=self.connect_and_scrape, args=())
        bt_thread.start()
    
    def stop_scraping(self):
        self.run=False

    def exit_gracefully(self, *args):
        self.run=False
        exit()

    #    await self.connect_and_scrape()      
#        asyncio.create_task(self.connect_and_scrape())      
      #  asyncio.get_event_loop().run_until_complete(self.connect_and_scrape())

if __name__ == "__main__":
    jk = JkBmsBle("C8:47:8C:E4:54:0E")  
    info("sss")

    jk.start_scraping()
    while True:
        print("asdf")
        print(jk.get_status())
        time.sleep(2)
