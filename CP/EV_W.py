import os, time, requests, sys, threading
from urllib.parse import quote
ult_mod=0
api_key=""
ciudades=[]
file="weather_config.txt"
api_central = os.getenv("API_CENTRAL_URL", "http://localhost:5001")

central_down = False

def central_mark_down():
    global central_down
    if not central_down:
        central_down=True
        print("ERROR: API_Central no disponible.")

def central_mark_up():
    global central_down
    if central_down:
        central_down=False
        print("API_Central recuperada.")

# Funcion que lee la configuracion del fichero.
def leer_config():
    api=""
    lista=[]
    try:
        f=open(file,"r",encoding="utf-8")
        lineas=f.readlines()
        f.close()
        if (len(lineas)>0):
            api=lineas[0].strip()
        for i in range(1,len(lineas)):
            c=lineas[i].strip()
            if(c!=""):
                lista.append(c)
    except FileNotFoundError:
        pass
    return api, lista

# Funcion que guarda la configuracion en el fichero.
def guardar_config(api, lista):
    try:
        f=open(file,"w",encoding="utf-8")
        f.write(api.strip()+"\n")
        for i in lista:
            i=i.strip()
            if(i!=""):
                f.write(i+"\n")
        f.close()
        print("Configuracion guardada correctamente en", file)
    except Exception as e:
        print("ERROR, no se pudo guardar la configuracion:", e)

def menu():
    while True:
        api_cfg, ciudades_cfg=leer_config()
        print("\nMenu:")
        if(api_cfg!=""):
            print("API key actual:", api_cfg)
        else:
            print("API key actual: Vacia.")
        if (len(ciudades_cfg)==0):
            print("Ciudades registradas: Ninguna.")
        else:
            print("Ciudades registradas:", ciudades_cfg)
        cps=None
        try:
            r=requests.get(api_central+"/api/cps", timeout=5)
            if r.status_code==200:
                cps=r.json()
                central_mark_up()
        except Exception as e:
            cps=None
            central_mark_down()
        print("CPs en la base de datos:")
        if (cps is None) or (len(cps)==0):
            print("Ningun CP disponible en la BD.")
        else:
            for cp in cps:
                print("  id:", cp.get("id"), "localizacion:", cp.get("localizacion"))
        print("\n1) Cambiar API key")
        print("2) Cambiar ciudad de un CP")
        print("0) Salir del menu")
        op=input("Opcion: ").strip()
        # Cambiar la API Key
        if op=="1":
            nueva_api=input("Nueva API key: ").strip()
            guardar_config(nueva_api, ciudades_cfg)
        # Cambiar la ciudad de un CP en la BD
        elif op=="2":
            
            cps=None
            try:
                r=requests.get(api_central+"/api/cps", timeout=5)
                if r.status_code==200:
                    cps=r.json()
            except Exception as e:
                cps=None
                print("ERROR, no se han podido leer los CPs desde API_Central:", e)
            if (cps is None) or (len(cps)==0):
                print("ERROR, no hay ninguna CP para cambiar.")
                continue
            id_txt=input("Id del CP a cambiar: ").strip()
            try:
                id_cp=int(id_txt)
            except ValueError:
                print("ERROR, id no valido.")
                continue

            nueva_ciudad=input("Nueva ciudad: ").strip()
            if (nueva_ciudad==""):
                print("ERROR, ciudad vacia.")
                continue

            payload={}
            payload["id"]=id_cp
            payload["ciudad"]=nueva_ciudad

            try:
                r2=requests.put(api_central+"/api/cp_ciudad", json=payload, timeout=5)
                if r2.status_code==204:
                    print("Ciudad del CP cambiada correctamente.")
                    if (nueva_ciudad not in ciudades_cfg):
                        ciudades_cfg.append(nueva_ciudad)
                        guardar_config(api_cfg, ciudades_cfg)
                elif r2.status_code==404:
                    print("ERROR, no existe ningun CP con esta id.")
                else:
                    print("ERROR, no se pudo cambiar la ciudad del CP, codigo HTTP:", r2.status_code)
            except Exception as e:
                central_mark_down()
                print("ERROR, no se pudo llamar a API_Central para cambiar la ciudad.")
        elif op=="0":
            print("Saliendo del menu.")
            break


threading.Thread(target=menu, daemon=True).start()
while True:
    try:
        info=os.stat(file)
    except FileNotFoundError:
        time.sleep(4)
        continue

    if(info.st_mtime!=ult_mod):
        ult_mod=info.st_mtime
        api_key=""
        ciudades=[]
        f=open(file,"r", encoding="utf-8")
        lineas=f.readlines()
        f.close()
        if (len(lineas)>0):
            api_key=lineas[0].strip()
        for i in range(1, len(lineas)):
            ciudad=lineas[i].strip()
            if (ciudad!=""):
                ciudades.append(ciudad)
    # Si no hay API Key.
    if(api_key==""):
        time.sleep(4)
        continue
    for i in ciudades:
        try:
            url="https://api.openweathermap.org/data/2.5/weather?q="+quote(i)+"&appid="+api_key+"&units=metric"
            req=requests.get(url, timeout=5)
            data=req.json()
            main=data.get("main", {})
            temp=main.get("temp", None)
            if temp is None:
                continue
            temp=float(temp)
        except Exception as e:
            print("ERROR, procesando ciudad", i, "->", e)
            continue
        url_api=api_central+"/api/openweather"
        payload={"ciudad": i, "temp": temp}
        try:
            requests.put(url_api, json=payload, timeout=5)
            central_mark_up()
        except Exception:
            central_mark_down()
            break
    time.sleep(4)
