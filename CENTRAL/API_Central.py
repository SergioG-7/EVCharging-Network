from flask import Flask, request, jsonify
import mysql.connector
import os
# Crea la app.
app=Flask(__name__)

# Funcion que abre la conexion con la BD.
def get_db_connection():
    # Carga configuracion de la BD.
    host=os.getenv("DB_HOST", "db")
    user=os.getenv("DB_USER", "root")
    password=os.getenv("DB_PASS", "1234")
    database=os.getenv("DB_NAME", "evcharging")
    port=int(os.getenv("DB_PORT", "3306"))
    # Crea conexion.
    conn=mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
    )
    return conn

@app.route('/api/cps', methods=['GET'])
def get_cps():
    try:
        conn=get_db_connection()
        cur=conn.cursor()
        cur.execute('SELECT id, localizacion, precio, estado, token FROM charging_point')
        data=cur.fetchall()
        cur.close()
        conn.close()
        cps=[]
        for i in data:
            cp={}
            cp["id"]=i[0]
            cp["localizacion"]=i[1]
            cp["precio"]=float(i[2])
            cp["estado"]=i[3]
            cp["token"]=i[4]
            cps.append(cp)
        return jsonify(cps), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/cp_ciudad', methods=['PUT'])
def cambiar_ciudad_cp():
    try:
        data=request.get_json()
        if (data is None):
            return "", 400
        id_cp=data.get("id", None)
        ciudad=str(data.get("ciudad","")).strip()
        if (id_cp is None) or (ciudad==""):
            return "", 400
        try:
            id_cp=int(id_cp)
        except ValueError:
            return "", 400
        conn=get_db_connection()
        cur=conn.cursor()
        cur.execute("UPDATE charging_point SET localizacion=%s WHERE id=%s", (ciudad, id_cp))
        conn.commit()
        filas=cur.rowcount
        cur.close()
        conn.close()
        if filas==0:
            return "", 404
        return "", 204
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/openweather', methods=['PUT'])
def put_openweather():
    try:
        data=request.get_json()
        if (data is None):
            return "", 400
        ciudad=str(data.get("ciudad","")).strip()
        temp=data.get("temp", None)
        if (ciudad=="") or (temp is None):
            return "", 400
        temp=float(temp)
        conn=get_db_connection()
        cur=conn.cursor()
        cur.execute("INSERT INTO openweather (ciudad, temp) VALUES (%s, %s) ON DUPLICATE KEY UPDATE temp=%s", (ciudad, temp, temp))
        conn.commit()
        cur.close()
        conn.close()
        return "", 204
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# FUncion que lee los datos del clima.
@app.route('/api/openweather', methods=['GET'])
def get_openweather():
    try:
        conn=get_db_connection()
        cur=conn.cursor()
        cur.execute("SELECT ciudad, temp FROM openweather ORDER BY ciudad")
        data=cur.fetchall()
        cur.close()
        conn.close()
        lista=[]
        for i in data:
            item={}
            item["ciudad"]=i[0]
            item["temp"]=float(i[1])
            lista.append(item)
        return jsonify(lista), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/drivers', methods=['GET'])
def get_drivers():
    try:
        conn=get_db_connection()
        cur=conn.cursor()
        cur.execute("SELECT id FROM driver ORDER BY id")
        data=cur.fetchall()
        cur.close()
        conn.close()
        drivers=[]
        for i in data:
            driver={}
            driver["id"]=i[0]
            drivers.append(driver)
        return jsonify(drivers), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/logs', methods=['GET'])
def get_logs():
    try:
        path=os.getenv("CENTRAL_LOG_PATH", "/app/central.log")
        if not os.path.exists(path):
            return jsonify({"lines": []}), 200
        f=open(path, "r", encoding="utf-8", errors="ignore")
        lineas=f.readlines()
        f.close()
        ultimas=lineas[-100:]
        limpio=[]
        for i in ultimas:
            limpio.append(i.rstrip("\n"))

        return jsonify({"lines": limpio}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__=="__main__":
    app.debug=True
    app.run(host="0.0.0.0", port=5001)