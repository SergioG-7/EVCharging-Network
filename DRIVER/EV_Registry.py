from flask import Flask, request, jsonify
import secrets
import ssl
import threading


app = Flask(__name__)
PORT = 8443
CERT_FILE = 'certServ.pem' 


registered_cps = {}
lock_db = threading.Lock()


@app.route('/register', methods=['POST'])
def register_cp():
    try:
        data = request.get_json()

        if not data or 'id' not in data or 'location' not in data:
            return jsonify({'status': 'ERROR', 'message': 'Faltan datos'}), 400
            
        cp_id = str(data['id'])
        location = data['location']
        
        #Generar token
        token = "TK-" + secrets.token_hex(8).upper()
        
        with lock_db:
            registered_cps[cp_id] = {
                'location': location,
                'token': token,
                'status': 'ACTIVE',
            }
        
        print(f"[REGISTRY] CP {cp_id} registrado via API REST Segura. Token: {token}")
        
        return jsonify({
            'status': 'OK',
            'token': token
        }), 200
        
    except Exception as e:
        print(f"[ERROR] {e}")
        return jsonify({'status': 'ERROR', 'message': str(e)}), 500


@app.route('/register/<cp_id>', methods=['DELETE'])
def unregister_cp(cp_id):
    with lock_db:
        if cp_id in registered_cps:
            del registered_cps[cp_id]
            print(f"[REGISTRY] CP {cp_id} eliminado.")
            return jsonify({'status': 'OK', 'message': 'CP dado de baja'}), 200
        else:
            return jsonify({'status': 'ERROR', 'message': 'CP no encontrado'}), 404

def main():
    print(f"[EV_REGISTRY] Iniciando API REST Segura en puerto {PORT}...")
    
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        context.load_cert_chain(CERT_FILE, CERT_FILE)
    except FileNotFoundError:
        print(f"[ERROR] No se encuentra '{CERT_FILE}'.")
        return

    #Servidor Flask con SSL
    app.run(host='0.0.0.0', port=PORT, ssl_context=context, debug=False)

if __name__ == "__main__":
    main()