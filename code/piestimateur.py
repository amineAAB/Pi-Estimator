import findspark
findspark.init(" ")
import pyspark 
findspark.find()

from pyspark import SparkContext,SparkConf
import numpy as np
from random import random
from time import time
import math
from operator import add


#### Initialisation de Spark
conf = pyspark.SparkConf().setAppName('piestimateur').setMaster('local')
sc = pyspark.SparkContext(conf = conf)


def is_point_inside_unit_circle(p):
    ### simuler un point p avec deux coordonnées x et y pour déterminer
    ### si le point simulé est à l’intérieur ou à l'extérieur du cercle.
    x, y = random(), random()         ### simuler deux point  x et y
    return 1 if x*x + y*y < 1 else 0  ### vérifier si ces deux point sont dans la cercle


def pi_estimator_spark(n): 
    ### Cette fonction permet d'estimer pi avec Spark
    ### Elle retourne l'approximation de pi
    count = sc.parallelize(range(0, n))
    n_in = count.map(is_point_inside_unit_circle).reduce(add)
    pi = (4 * n_in) / n                   ### calcul de l'approximation de pi
    return  pi
    

def pi_estimator_numpy(n):
    ### Cette fonction permet d'estimer pi avec numpy
    ### Retourne l'approximation de pi
    count = np.zeros(n)
    for i in range(n) :
        count[i] = is_point_inside_unit_circle(1)
        
    n_in = np.sum(count)
    pi = (4 * n_in) / n                   ### calcul de l'approximation de pi
    return  pi    
        
        
####   le nombre de points
n=1000000       

### Calcul du temps d'execution avec spark
t0 = time()
pi_spark = pi_estimator_spark(n)
t1 = time()
print('n=', n)
print('l approximation  de pi avec spark est',pi_spark)
print('Le temps d exécution de l algorithme avec spark est ',t1-t0)
print('L erreur entre la valeur exacte de pi est la valeur approximale avec spark est ',np.abs(math.pi- pi_spark))

### Calcul du temps d'execution avec numpy
t0 = time()
pi_numpy = pi_estimator_numpy(n)
t1 = time()
print('l approximation  de pi avec numpy est',pi_numpy)
print('Le temps d exécution de l algorithme avec numpy est ',t1-t0)
print('L erreur entre la valeur exacte de pi est la valeur approximale avec numpy est ',np.abs(math.pi- pi_numpy))


sc.stop() #Fermeture du SparkContext
