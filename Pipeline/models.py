#!/usr/bin/python
try:
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, String
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."    

Base = declarative_base()

class AverageSubtractedImages(Base):
    __tablename__ = 'average_subtracted_images'
    id = Column(Integer, primary_key=True)

    average_subtracted_location = Column(String(250))
    porod_volume = Column(String(250))
    
    def __init__(self, average_subtracted_location, porod_volume):
        self.average_subtracted_location = average_subtracted_location
        self.porod_volume = porod_volume
        
    def __repr__ (self):
        return "<AverageSubtractedImages('%s','%s')>" % (self.average_subtracted_location, self.porod_volume)


class DamVolumes(Base):
    __tablename__ = 'dam_volumes'
    id = Column(Integer, primary_key=True)
    
    dammif_pdb_file = Column(String(250))
    dam_volume = Column(String(250))
    average_subtracted_images_fk = Column(String(250))
    
    def __init__(self, dammif_pdb_file, dam_volume, average_subtracted_images_fk):
        self.dammif_pdb_file = dammif_pdb_file
        self.dam_volume = dam_volume
        self.average_subtracted_images_fk = average_subtracted_images_fk
        
    def __repr__ (self):
        return "<DamVolumes('%s','%s')>" % (self.dammif_pdb_file, self.dam_volume, self.average_subtracted_images_fk)
