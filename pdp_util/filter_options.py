from mddb import *

class FilterOptions(object):
    """
    The main class that represents an all filtering options
    """
    
    def __init__(self, t='raster', ensemble=None, options=[], **kwargs):
        self.t = t
        if self.t == 'raster':
            assert ensemble, "You must specify an ensemble name when creating filter options for a raster map"
            self.m = Mddb(ensemble, **kwargs)
        else:
            #TODO: create filter options for crmp map
            pass

    def test(self):
        self.m.test()

    def get_options(self):
        if self.t == 'raster':
            return self.raster_options()
        else: return self.crmp_options()

    def crmp_options(self):
        #TODO: fetch crmp options
        pass

    def raster_options(self):
        options=[]
        #TODO: adapt to given option list to get those options
        # ds = Option(label='Downscaling Method', div_id='d_method', t='select')
        # ds.options=self.m.downscaling_methods
        # print ds
        es = Option(label='Emission Scenarios', div_id='scenario', t='select')
        es.options = self.m.emission_scenarios

        models = Option(label='Models', div_id='model', t='select')
        models.options = self.m.models

        variables = Option(label='Variables', div_id='var', t='select')
        variables.options = self.m.variables

        runs = Option(label='Runs', div_id='run', t='select')
        runs.options=self.m.runs
        
        options=[es, models, runs, variables] 
        return options

class SelectList(object):
    def __init__(self):
        pass

class Option(object):
    """
    Base class for all option types
    """
    def __init__(self,
                 label=None,
                 div_id=None,
                 t=None,
                 **kwargs):
        self.div_id = div_id
        self.label = label
        self.type = t
        
