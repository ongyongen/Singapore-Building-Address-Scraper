import parameters
from parameters import *

class cleanFile:
    def __init__(self, df, pa):
        self.df = df
        self.pa = pa
        self.d_pa = {self.pa.loc[i,'PLN_AREA_N'] : self.pa.loc[i,'geometry'] for i in range(len(self.pa))}
        
    def get_df(self):
        return self.df
    
    def save_df(self, name_of_saved_file):
        return self.df.to_csv(name_of_saved_file)
    
    def remove_nil_in_building(self):
        self.df['building'] = list(map(lambda x: '' if x == 'NIL' else x, list(self.df['building'])))
        
    def format_address(self):
        self.df['address'] = list(map(lambda blk, road, building, postal:
                                   f"{building}, {blk} {road} SINGAPORE {postal}" if building != ''
                                   else f"{blk} {road} SINGAPORE {postal}",
                                   list(self.df['blk']), list(self.df['road']), list(self.df['building']), list(self.df['postal'])))
    
    def create_point_geom(self):
        self.df['point_geometry'] = list(map(lambda lat, lon: Point(lon,lat), list(self.df['lat']), list(self.df['lon'])))
    
    def extract_planning_area_for_all_entries(self):
        def find_planning_area(point):
            pa_lst = list(filter(lambda x: point.within(x[1]), list(self.d_pa.items())))
            return pa_lst[0][0]
        self.df['planning_area'] = list(map(lambda x: find_planning_area(x), list(self.df['point_geometry'])))
        
    def drop_irrelevant_columns(self):
        self.df = self.df.drop(columns=['Unnamed: 0','point_geometry'])
        
        
        
file = cleanFile(df, pa)
file.remove_nil_in_building()
file.format_address()
file.create_point_geom()
file.extract_planning_area_for_all_entries()
file.drop_irrelevant_columns()
file.get_df()
file.save_df(name_of_saved_file)