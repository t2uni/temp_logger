import csv
from datetime import datetime
from paho.mqtt.client import Client, MQTTMessage
import os
import time
import json

DEBUG = True

def debug_print(message: str) -> None:
    if DEBUG:
        print("DEBUG:", message)


class TempLogger:
    """Log temperature received via MQTT.

    Always user in with ... as ... construct!

    Attributes:
        _log_file
        _client
    """

    TEMP_MESSAGE_KEYS = ["temperature", "resistance", "timestamp"]
    FLOW_MESSAGE_KEYS = ["temperature", "volflow", "massflow", "pressure", "setpoint", "timestamp"]
    PRESSURE_MESSAGE_KEYS = ["timestamp", "pressure"]
    CSV_DELIM = " "

    def __init__(self, file_path_temp: str, file_path_flow: str, file_path_pressure: str) -> None:
        self._log_file_temp = open(file_path_temp, "a")
        self._log_file_flow = open(file_path_flow, "a")
        self._log_file_pressure = open(file_path_pressure, "a")
        
        if os.stat(file_path_temp).st_size == 0:  # Write header only into new files
            self._write_header_temp()
        if os.stat(file_path_flow).st_size == 0:
            self._write_header_flow()
        if os.stat(file_path_pressure).st_size == 0:
            self._write_header_pressure()
            

        self._client = Client()
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

        self._client.username_pw_set("ald", "ald2017")
        self._client.connect("ald", port=1883, keepalive=60)

        debug_print("Initialised.")

    def __enter__(self):
        self._client.loop_start()
        debug_print("Loop started.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.loop_stop()
        self._log_file_temp.close()
        self._log_file_flow.close()
        self._log_file_pressure.close()
        debug_print("Loop stopped.")

    def _on_connect(self, client: Client, userdata, flags, rc) -> None:
        client.subscribe("ald/sample/temperature")
        client.message_callback_add('ald/sample/temperature', self._on_sample_temperature)

        client.subscribe("ald/flow/state")
        client.message_callback_add('ald/flow/state', self._on_flow_state)       

        client.subscribe("ald/pressure/main")
        client.message_callback_add('ald/pressure/main', self._on_pressure_main) 

        debug_print("Connected.")

    def _on_message(self, client: Client, userdata, message: MQTTMessage) -> None:
        print('DEBUG', message.payload)

    def _on_flow_state(self, client: Client, userdata, message: MQTTMessage) -> None:
        values = json.loads(message.payload.decode())
        debug_print("Message received: {}".format(values))

        if values.keys() != set(self.FLOW_MESSAGE_KEYS):
            print("ERROR: Malformed message! Skipping this one.",
                  "Keys of message:", values.keys())
            return
        else:
            temp = values[self.FLOW_MESSAGE_KEYS[0]]
            volflow = values[self.FLOW_MESSAGE_KEYS[1]]
            massflow = values[self.FLOW_MESSAGE_KEYS[2]]
            pressure = values[self.FLOW_MESSAGE_KEYS[3]]
            setpoint = values[self.FLOW_MESSAGE_KEYS[4]]
            timestamp = values[self.FLOW_MESSAGE_KEYS[5]]
            self._write_data_point_flow(temp, volflow, massflow, pressure, setpoint, timestamp)
         
    def _on_pressure_main(self, client: Client, userdata, message: MQTTMessage) -> None:
        values = json.loads(message.payload.decode())
        debug_print("Message received: {}".format(values))

        if values.keys() != set(self.PRESSURE_MESSAGE_KEYS):
            print("ERROR: Malformed message! Skipping this one.",
                  "Keys of message:", values.keys())
            return
        else:
            timestamp = values[self.PRESSURE_MESSAGE_KEYS[0]]
            pressure = values[self.PRESSURE_MESSAGE_KEYS[1]]
            self._write_data_point_pressure(timestamp, pressure)
            

    def _on_sample_temperature(self, client: Client, userdata, message: MQTTMessage) -> None:
        values = json.loads(message.payload.decode())
        debug_print("Message received: {}".format(values))

        if values.keys() != set(self.TEMP_MESSAGE_KEYS):
            print("ERROR: Malformed message! Skipping this one.",
                  "Keys of message:", values.keys())
            return
        else:
            temp = values[self.TEMP_MESSAGE_KEYS[0]]
            resistance = values[self.TEMP_MESSAGE_KEYS[1]]
            datetime = values[self.TEMP_MESSAGE_KEYS[2]]
            self._write_data_point_temp(temp, resistance, datetime)

    def _write_header_temp(self) -> None:
        writer = csv.writer(self._log_file_temp, delimiter=self.CSV_DELIM)
        writer.writerow(["Temperature", "Resistance", "Datetime"])
        
    def _write_header_flow(self) -> None:
        writer = csv.writer(self._log_file_flow, delimiter=self.CSV_DELIM)
        writer.writerow(["Temperature", "Volflow", "Massflow", "Pressure", "Setpoint", "Datetime"])
        
    def _write_header_pressure(self) -> None:
        writer = csv.writer(self._log_file_pressure, delimiter=self.CSV_DELIM)
        writer.writerow(["Datetime", "Pressure"])

    def _write_data_point_temp(self, temperature: str, resistance: str, datetime: str) -> None:
        writer = csv.writer(self._log_file_temp, delimiter=self.CSV_DELIM)
        writer.writerow([temperature, resistance, datetime])
        self._log_file_temp.flush()
        
    def _write_data_point_flow(self, temp: str, volflow: str, massflow: str,
                               pressure: str, setpoint: str, timestamp: str) -> None:
        writer = csv.writer(self._log_file_flow, delimiter=self.CSV_DELIM)
        writer.writerow([temp, volflow, massflow, pressure, setpoint, timestamp])
        self._log_file_flow.flush()
        
    def _write_data_point_pressure(self, timestamp: str, pressure: str) -> None:
        writer = csv.writer(self._log_file_pressure, delimiter=self.CSV_DELIM)
        writer.writerow([timestamp, pressure])
        self._log_file_pressure.flush()
    
    


from time import sleep
with TempLogger("sample_temperatures.dat", "flow.dat", "pressure.dat") as logger:
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        pass
