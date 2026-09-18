import time
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from chargingpoint import ChargingPoint

class Database:
    def __init__(self, host="db", port=3306, user="root", password="1234", database="evcharging",
                 retries=30, delay=2):
        self.conn = None
        last_err = None
        for i in range(1, retries + 1):
            try:
                self.conn = mysql.connector.connect(
                    host=host, port=port, user=user, password=password, database=database
                )
                self.cursor = self.conn.cursor(dictionary=True)
                print(f"[DB] Conexión establecida a {host}:{port} (intento {i})")
                break
            except Error as e:
                last_err = e
                print(f"[DB] No disponible (intento {i}/{retries}): {e}")
                time.sleep(delay)
        if not self.conn:
            raise RuntimeError(f"[DB] No se pudo conectar a MySQL: {last_err}")

    def register_cp(self, cp_id, localizacion, precio=0.30,token=None):
        sql = (
            "INSERT INTO charging_point (id, localizacion, precio, estado, token) "
            "VALUES (%s,%s,%s,'DESCONECTADO',%s) "
            "ON DUPLICATE KEY UPDATE localizacion=VALUES(localizacion), precio=VALUES(precio), estado='ACTIVADO', token=VALUES(token)"
        )
        self.cursor.execute(sql, (cp_id, localizacion, precio, token))
        self.conn.commit()

    def update_cp_status(self, cp_id, estado):
        self.cursor.execute("UPDATE charging_point SET estado=%s WHERE id=%s", (estado, cp_id))
        self.conn.commit()

    # Obtener todos los CPs
    def getAllCPs(self):
        self.cursor.execute("SELECT id, localizacion, precio, estado, token FROM charging_point")
        filas=self.cursor.fetchall()
        cps={}
        for i in filas:
            cps[i["id"]]=ChargingPoint(i["id"], i["localizacion"], float(i["precio"]), i["estado"], i["token"])
        return cps

    # Obtener todos los Drivers
    def getAllDrivers(self):
        self.cursor.execute("SELECT id FROM driver")
        filas=self.cursor.fetchall()
        drivers=[]
        for i in filas:
            drivers.append(i["id"])
        return drivers

    # Registrar un Driver
    def register_driver(self, driver_id):
        self.cursor.execute(
            "INSERT INTO driver (id) VALUES (%s)",
            (int(driver_id),)
        )
        self.conn.commit()
    
    def getOpenWeather(self):
        self.conn.commit()
        self.cursor.execute("SELECT ciudad, temp FROM openweather ORDER BY ciudad")
        filas=self.cursor.fetchall()
        lista=[]
        for i in filas:
            item={}
            item["ciudad"]=i["ciudad"]
            item["temp"]=float(i["temp"])
            lista.append(item)
        return lista

    # Obtener los CPs de una ciudad concreta.
    def getCPsByCity(self, ciudad):
        self.conn.commit()
        self.cursor.execute(
            "SELECT id, estado FROM charging_point WHERE localizacion=%s",
            (ciudad,)
        )
        return self.cursor.fetchall()

    def close(self):
        try:
            self.cursor.close()
        finally:
            try:
                self.conn.close()
            except:
                pass
