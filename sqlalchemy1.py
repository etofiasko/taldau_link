from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from nlink import get_index_periods

engine = create_engine("postgresql://postgres:123456@localhost:5432/new_test_taldau")

Base = declarative_base()

class GDPprod(Base):
    __tablename__ = "gdp_production_method"

    id = Column(Integer, primary_key=True)
    value = Column(Integer)
    date = Column(Date)
    created = Column(Date)
    updated = Column(Date)
    date_period_id = Column(Integer, ForeignKey('date_periods.id'))
    index_period_id = Column(Integer, ForeignKey('index_periods.id'))
    region67_id = Column(Integer, ForeignKey('regions67.id'))
    
    regions67 = relationship("Region67", backref="gdp_production_method_data")
    index_periods = relationship("IndexPeriod", backref="gdp_production_method_data")
    date_periods = relationship("DatePeriod", backref="gdp_production_method_data")

    def __repr__(self):
        return f"<GDPprod(value='{self.value}'"


class Region67(Base):
    __tablename__ = "regions67"
    
    id = Column(Integer, primary_key=True)
    term_id = Column(Integer)
    name = Column(String)


class IndexPeriod(Base):
    __tablename__ = "index_periods"
    
    id = Column(Integer, primary_key=True)
    period_id = Column(Integer)
    name = Column(String)
    
    
class DatePeriod(Base):
    __tablename__ = "date_periods"
    
    id = Column(Integer, primary_key=True)
    date_id = Column(Integer)
    name = Column(String)
    index_id = Column(Integer, ForeignKey('indices.id'))
    
    indices = relationship("Index", backref="date_periods")


class Index(Base):
    __tablename__ = "indices"
    
    id = Column(Integer, primary_key=True)
    index_id = Column(Integer)
    name = Column(String)
    table_name = Column(String)


# Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)
session = Session()

index_info = get_index_periods(2709379)
index = Index(index_id=index_info["index_id"], name=index_info["name"], table_name="gdp_production_method")
session.add(index)

indices = session.query(Index).one()
print(indices)

session.close()
