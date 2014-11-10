# -*- coding: utf-8 -*-
import sys, csv, os, math

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, Unicode, Boolean
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError

# -----------------------------
# DataBase def with Sqlalchemy
# -----------------------------

db_file = os.path.join('db', 'proesis.sqlite')
engine = create_engine('sqlite:///'+db_file, echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Proesis(Base):
    __tablename__ = 'proesis'

    id = Column(Integer, primary_key=True)
    ga_itemid = Column(Unicode, default=u'')
    itemid = Column(Unicode, unique=True, index=True, nullable=False)
    title = Column(Unicode, default=u'')    
    quantity = Column(Integer, default=0)
    no_sold = Column(Integer, default=0)
    price = Column(Float, default=0.0) 
    shipping = Column(Float, default=0.0)
    closed = Column(Boolean, default=False)
    changes = Column(Integer, default=0)
    
    attr_bit = {'no_sold':2**2, 'price':2**1, 'shipping':2**0}    

    def set_change_for(self, *attributes):
        if self.changes is None: self.changes = 0
        for attr in attributes:
            if attr in self.attr_bit:
                self.changes |= self.attr_bit[attr]
            else: raise Exception('Error: attribute not exsist or not setable for changes')
            
    def reset_change_for(self, *attributes):
        if self.changes is None: self.changes = 0
        for attr in attributes:
            if attr in self.attr_bit:
                self.changes &= ~self.attr_bit[attr]
            else: raise Exception('Error: attribute not exsist or not resetable for changes')
    
                                  
Base.metadata.create_all(engine)

# ----------------------------------------
# A csv.DictWriter specialized with Fx csv
# ----------------------------------------

class EbayFx(csv.DictWriter):
    '''Subclass csv.DictWriter, define delimiter and quotechar and write headers'''
    def __init__(self, filename, fieldnames):
        self.fobj = open(filename, 'wb')
        csv.DictWriter.__init__(self, self.fobj, fieldnames, delimiter=';', quotechar='"')
        self.writeheader()
    def close(self,):
        self.fobj.close()
       
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.close()



# Constants

DATA_PATH = os.path.join('input_data')
ACTION = '*Action(SiteID=Italy|Country=IT|Currency=EUR|Version=745|CC=UTF-8)' # smartheaders CONST

# Fruitfull funcs

def filename_from(data_path):
    "Return the filename from data_path"
    t = list()
    for d in os.listdir(data_path):
        el = os.path.join(data_path, d)
        if os.path.isfile(el): t.append(el)
    if len(t)>1: raise Exception('File Error: more than one input file')
    elif len(t)==0: raise Exception('File Error: no input file')
    return t[0]


def loader(session):
    "Load ('itemid', 'title', 'quantity', 'no_sold', 'price', 'shipping') into db"
    input_fn = filename_from(DATA_PATH)
    input_clean_fn = os.path.join(DATA_PATH, 'cleaned.csv')
    if input_fn: # if no input file, just exit

        # clean " from description
        with open(input_fn, 'rb') as fin:
            with open(input_clean_fn, 'wb') as fout:
                for i, line in enumerate(fin):
                    fout.write('"'+line[1:-3].replace('***', '').replace('","', '*,*').replace('"', "''").replace('*,*', '","')+'"\n')
        os.remove(input_fn)   
        if i <= 0: raise Exception('File Error: no lines inside')     


        # init of all closed-ad-db ids
        closed_ad_db_ids = [id_tuple[0] for id_tuple in session.query(Proesis.id).all()]

        with open(input_clean_fn, 'rb') as f:
            csv_rows = csv.reader(f, delimiter=',', quotechar='"')
            csv_rows.next() # skip header
            db_row = dict()
            for row in csv_rows:
                try:
                    db_row['itemid']    =row[2]
                    db_row['title']     =row[3].decode('iso.8859-1')
                    db_row['quantity']  =int(row[4].strip())
                    db_row['no_sold']   =int(row[5].strip())
                    db_row['price']     =float(row[8].replace(',', '').strip()[7:])
                    db_row['shipping']  =float(row[9].replace(',', '').strip()[7:])

                    ad = session.query(Proesis).filter(Proesis.itemid == db_row['itemid']).first()
                    if ad: # exsist
                        closed_ad_db_ids.remove(ad.id) # because it is not old

                        if ad.no_sold != db_row['no_sold']: ad.set_change_for('no_sold')
                        if ad.price != db_row['price']:ad.set_change_for('price')
                        if ad.shipping != db_row['shipping']:ad.set_change_for('shipping')
                    else:
                        ad = Proesis()

                    for attr, value in db_row.items():
                        setattr(ad, attr, value)
                    session.add(ad)

                except ValueError:
                    print 'rejected line:'
                    print db_row
                    print sys.exc_info()[0]
                    print sys.exc_info()[1]
                    print sys.exc_info()[2]
        os.remove(input_clean_fn)
            
    for db_id in closed_ad_db_ids:
        closed_ad = session.query(Proesis).filter(Proesis.id == db_id).first()
        closed_ad.closed = True
        session.add(closed_ad)
    session.commit()

def load():
    s = Session()
    loader(s)
    s.close()

def revise_price(session=Session()):
    'Fx revise price, only for free shipping Items (ie price>30)'
    smartheaders=(ACTION, 'ItemID', 'StartPrice')
    arts = session.query(Proesis).filter(Proesis.ga_itemid !=u'', # you have it listed
                                         Proesis.price+Proesis.shipping > 30)
    output_fn = os.path.join('revise_super_price.csv')
    with EbayFx(output_fn, smartheaders) as wrt:
        for art in arts:
            fx_revise_row = {ACTION:'Revise',
                             'ItemID': art.ga_itemid,
                             'StartPrice': math.ceil(art.price+art.shipping)}
            wrt.writerow(fx_revise_row)
    session.close()


def reset_closed(s=Session()):
    ''
    ads = s.query(Proesis)
    for ad in ads:
        ad.closed = False
        s.add(ad)
    s.commit()
    s.close()  

def reset_changes(s=Session()):
    ''
    ads = s.query(Proesis)
    for ad in ads:
        ad.changes = 0
        s.add(ad)
    s.commit()
    s.close()

load()
#revise_price()
#reset_closed()
#reset_changes()


