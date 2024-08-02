import pika
import csv
import requests
import time
import multiprocessing




bucket_name = "CAN"
measurement_name = "Can_data"
influx_url = "yur influxurl/api/v2/write?org=your_org_id&bucket=" + bucket_name
token = "replace with your influx api token"

gateway_id_queue = "available_gateway_ids"

user = "Bhargav"
password = "admin"
    
def send_data_to_influxdb(data):
    print("data:" + data)
    headers = {
        "Authorization": "Token " + token,
        "Content-Type": "text/plain"
    }
    url = influx_url + "&precision=s"
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 204:
       print("Data sent successfully.")
    else:
        print("Failed to send data. Status code:", response.status_code)
	

def line_protcol_maker(csv_line,gatewayid):
    row = csv_line.split(',')
    line_protocol = f"{measurement_name},Gateway_id={gatewayid} {row[0].replace(' ', '_')}={row[1]}\n"
    return line_protocol


def get_all_gateway_ids():
    gateway_id_list = []
    def send_to_rabbitmq():
        queue_name = gateway_id_queue
        credentials = pika.PlainCredentials(user, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1", credentials=credentials))
        channel = connection.channel()
        method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=True)
        if method_frame:
            return body.decode()
        else:
            return "NO DATA!"

    while True:
        id = send_to_rabbitmq()
        if(id == "NO DATA!"):
            break;
        else:
            gateway_id_list.append(id)
    return gateway_id_list


def send_data_of_a_gateway_to_influx(gateway_id):
    def send_to_rabbitmq(gateway_id):
        credentials = pika.PlainCredentials(user, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1", credentials=credentials))
        channel = connection.channel()
        method_frame, header_frame, body = channel.basic_get(queue=gateway_id, auto_ack=True)
        if method_frame:
            return body.decode()
        else:
            return "NO DATA!"

    while True:
        queue_content = send_to_rabbitmq(gateway_id)
        if queue_content!="NO DATA!":
            send_data_to_influxdb(line_protcol_maker(queue_content,gateway_id))
    




    
    
def simulate_Rabbit_to_cloud_sender():
    processes = []
    available_gateway_id_list = get_all_gateway_ids()
    
    for id in available_gateway_id_list:
        process = multiprocessing.Process(target=send_data_of_a_gateway_to_influx, args=(str(id),))
        processes.append(process)
        process.start()
    

    for process in processes:
        process.join()




#first wait until data is sent to the server or else we will misss some gateway ids
print("VM HANDLER RUNNING")
simulate_Rabbit_to_cloud_sender()
