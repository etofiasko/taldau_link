from datetime import datetime
import requests
import time
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from nlink import get_index_periods

engine = create_engine("postgresql://postgres:123456@localhost:5432/new_test_taldau2")

Base = declarative_base()

# какой тип данных выбрать для value?
# нужен ли updated
# index_period_id = Column(Integer, ForeignKey('index_periods.id'))
# index_periods = relationship("IndexPeriod", backref="gdp_production_method_data")
class GDPprod(Base):
    __tablename__ = "gdp_production_method"

    id = Column(Integer, primary_key=True)
    
    value = Column(Float)
    date = Column(Date)
    created = Column(Date)
    date_period_id = Column(Integer, ForeignKey('date_periods.id'))
    region67_id = Column(Integer, ForeignKey('regions67.id'))
    
    date_periods = relationship("DatePeriod", backref="gdp_production_method_data")
    regions67 = relationship("Region67", backref="gdp_production_method_data")

    def __repr__(self):
        return f"<GDPprod(value='{self.value}'"


class Region67(Base):
    __tablename__ = "regions67"
    
    id = Column(Integer, primary_key=True)
    name = Column(String)


class IndexPeriod(Base):
    __tablename__ = "index_periods"
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    
    
class DatePeriod(Base):
    __tablename__ = "date_periods"
    
    id = Column(Integer, primary_key=True)
    date_id = Column(Integer)
    name = Column(String)
    index_id = Column(Integer, ForeignKey('indices.id'))
    index_period_id = Column(Integer, ForeignKey('index_periods.id'))
    
    index_periods = relationship("IndexPeriod", backref="date_periods")
    indices = relationship("Index", backref="date_periods")


class Index(Base):
    __tablename__ = "indices"
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    table_name = Column(String)


Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)
session = Session()

index_info = get_index_periods(2709379)
index = Index(id=int(index_info["index_id"]), name=index_info["name"], table_name="gdp_production_method")
session.add(index)

indices = session.query(Index).all()

urls = []
# здесь нужно будет проверить появились ли новые периоды, добавить их и сохранить для использовании при загрузке
for period in index_info["periods"]:
    index_period = IndexPeriod(id=int(period["period"]["id"]), name=period["period"]["name"])
    session.add(index_period)
    
    date_ids = set(period["dates"]["datesIds"])
    date_ids_in_db = set(session.query(DatePeriod.date_id).filter_by(index_id=index_info["index_id"],index_period_id=period["period"]["id"]).all())
    date_differences = list(date_ids.symmetric_difference(date_ids_in_db))
    
    date_str = [str(num) for num in date_differences]
    dates_for_url = ','.join(date_str)
    
    for date_id, date_name in zip(period["dates"]["datesIds"], period["dates"]["periodNameList"]):
        date_period = DatePeriod(date_id=int(date_id), name=date_name, index_id=int(index_info["index_id"]), index_period_id=int(period["period"]["id"]))
        session.add(date_period)
    
    url = f"https://taldau.stat.gov.kz/ru/Api/GetIndexData/{index_info['index_id']}?period={period['period']['id']}&dics={period['dic_ids']}&dateIds={dates_for_url}"
    urls.append([url, period['period']['id']])

print(urls)


for url in urls:
    time.sleep(2)
    response = requests.get(url[0])
    if response.status_code == 200:
        index_data = response.json()
    else:
        print(response.status_code)
        break

    for terms in index_data:
        region67 = session.query(Region67).filter_by(name=terms["termNames"][0]).all()
        if not region67:
            region67 = Region67(id=int(terms["terms"][0]), name=terms["termNames"][0])
            session.add(region67)
        else:
            region67 = region67[0]

        for data in terms["periods"]:
            date = datetime.strptime(data["date"], "%d.%m.%Y")
            created = datetime.now()
            index_period_id = int(url[1])
            date_period_id = session.query(DatePeriod).filter_by(index_period_id=index_period_id, name=data["name"]).all()[0].id
            gdpprod = GDPprod(value=data["value"], date=date, created=created, date_period_id=date_period_id, region67_id=region67.id)
            session.add(gdpprod)

session.commit()
session.close()
