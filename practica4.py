from pyspark import SparkContext
import json
import datetime
import matplotlib.pyplot as plt

def convert_date(date):
    if isinstance(date, str):
        date = date.replace("Z","+0000")
        date = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
    else:
        date = date['$date']
        date = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f%z")
    return (date.month,date.hour)

def mapper(line):
    data = json.loads(line)
    usert = data['user_type']
    time = data['travel_time']
    date = convert_date(data['unplug_hourTime'])
    return usert, time, date

def text_File(json):
    sc = SparkContext() 
    try:
        rdd_base = sc.textFile(json)

        if not type(json) is str: 
            raise TypeError("Only strings are allowed") 
            
        if json[-1] != 'n' or json[-2] != 'o' or json[-3] != 'o' or json[-4] != 's' or json[-5] != 'j' or json[-6] != '.': 
            raise TypeError("Watch out. The file extension is not correct.")
    
    finally: 
        print("OK")
        return rdd_base

def total_travel(rdd_base): 
    rdd = rdd_base.map(mapper)
    rdd.countByKey()
    n0 = rdd.count()
    return rdd, n0

def filter_user(rdd, user_type):
    return rdd.filter(lambda x: x[0]==user_type).map(lambda x: ((x[2][0],x[2][1]),x[1]))
                                                            #(mes, hora 24h), tiempo seg.
def filter_month(rdd, month):#Aplicar siempre después de filter_user
    return rdd.filter(lambda x: x[0][0]==month).map(lambda x: (x[0][1],x[1]))

def filter_month_and_user(rdd, user, month):#aplicar sobre el rdd base
    return rdd.filter(lambda x: (x[0]==user and x[2][0]==month)).map(lambda x: ((x[2][0],x[2][1]),x[1]))

def mean(rdd):#calculo de la media, una vez aplicado filter_user
    contador=(0,0)
    return rdd.aggregateByKey(contador, lambda x,y: (x[0]+y, x[1]+1), lambda x,y: (x[0]+y[0], x[1]+y[1])).\
    map(lambda x: (x[0], x[1][0]/x[1][1]))

def plot_pie(n0, n1, n2, n3):
    #Relación de la cantidad de viajes por tipo de usuario
    plt.pie([n0-(n1+n2+n3),n1,n2,n3], labels=['Sin determinar','Habitual','Ocasional','Empresa'], autopct='%1.1f%%',)

def main(json): 
    rdd_base = text_File(json)
    
    rdd, n0 = total_travel(rdd_base)
    
    print(f"Total travel: {n0} \n")
    
    print("A continuación se filtran los datos para hacer un estudio por usuario. \n")
    
    rdd_user1 = filter_user(rdd, 1)
    n1 = rdd_user1.count()
    print(f"Total de viajes con pase anual: {n1}\n")
    
    rdd_user2 = filter_user(rdd, 2)
    n2 = rdd_user2.count()
    print(f"Total de viajes con pase anual: {n2}\n")
    
    rdd_user3 = filter_user(rdd, 3)
    n3 = rdd_user3.count()
    print(f"Total de viajes con pase anual: {n3}\n")

    rdd_user2.countByKey()
    print("Número de viajes por horas: (mes, hora), total")

    rdd_mean2 = mean(rdd_user2)
    test = rdd_mean2.sortByKey().take(2)
    print(f"Test: {test}")

    #rdd_user2jun = filter_month(rdd_user2, 6)
    #rdd_user2jun.countByKey()
    #rdd_user2junmean = mean(rdd_user2jun)
    #rdd_user2junmean.sortByKey()

if __name__ == '__main__':
    json = input("Introduce el nombre del archivo json con el que quieres trabajar. \n Ten en cuenta que si el archivo es demasiado grande y no se utiliza un cluster es probable que se produzca un error.\n NOMBRE DEL ARCHIVO: ")
    main(json)