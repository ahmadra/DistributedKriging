import numpy as np
from pykrige import core as pc
from pykrige import variogram_models as vm
from ast import literal_eval as make_tuple

from sys import argv


def main():
    with open (argv[1],'r') as datafile:
        data = datafile.readlines()
    fitData = [make_tuple(x.rstrip('\n')) for x in data]
    lags = np.array([item[0] for item in fitData])
    semivariance = np.array([item[1] for item in fitData])
    variogram_model = 'exponential'
    variogram_function = vm.exponential_variogram_model
    variogram_model_parameters = pc.calculate_variogram_model(lags, semivariance, variogram_model, variogram_function, weight=True)
    np.savetxt(argv[2],variogram_model_parameters)

if __name__ == '__main__':
    main()