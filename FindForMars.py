def namestr(obj, namespace):
    return [name for name in namespace if namespace[name] is obj]
def FindForMars(func,mean,std,stdNum=2):
    if not func.rms:
        print("Function attribute 'RMS' not found. Function error not propagated")
        func.rms = 0
    MarsVal = func(mean)
    bound=abs(func(mean+(std*stdNum))+func.rms-func(mean))
    if func(100)>func(-100):
        lower=func(mean-(std*stdNum))-func.rms
        upper=func(mean+(std*stdNum))+func.rms
    elif func(100)<func(-100):
        upper=func(mean-(std*stdNum))-func.rms
        lower=func(mean+(std*stdNum))+func.rms
    else:
        print('What kind of function is this??')
        lower=func(mean-(std*stdNum))-func.rms
        upper=func(mean+(std*stdNum))+func.rms
    name = namestr(func,globals())[0]
    print(name+': '+"%.4f"%MarsVal+'+/-'+"%.4f"%bound+'\nLower:'+"%.4f"%lower+'\nUpper:'+"%.4f"%upper)
    return (MarsVal,bound)
    
strength = lambda x: 1091-128.2*x
density = lambda x: 1.32 - 0.01282*x
porosity = lambda x: 46.62 + 0.6568*x
strength.rms = 197.2
density.rms = 0.01
porosity.rms = 0.59

#CPP:
cpp = 10**0.570811752279008
cpp_s = 10**0.261349044276834

#MFF:
mff = 10**1.32456706506176
mff_s = 10**0.320791737480903

FindForMars(strength,cpp,cpp_s)
FindForMars(strength,mff,mff_s)
FindForMars(density,cpp,cpp_s)
FindForMars(density,mff,mff_s)
FindForMars(porosity,cpp,cpp_s)
FindForMars(porosity,mff,mff_s)

Strength_afo_Density = lambda x: -12482+10298*x
Strength_afo_Density.rms = 136.9
Strength_afo_Porosity = lambda x: 10395-199.3*x
Strength_afo_Density.rms = 153.9

#list(map(Strength_afo_Density,FindForMars(density,mff,mff_s)))
#list(map(Strength_afo_Porosity,FindForMars(porosity,mff,mff_s)))
