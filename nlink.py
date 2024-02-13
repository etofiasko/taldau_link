import requests
import time


def get_index_periods(index_id):
    data_link = {}
    data_link["index_id"] = index_id
    url_get_period = f"http://taldau.stat.gov.kz/ru/Api/GetPeriodList?indexId={index_id}"
    time.sleep(2)
    response = requests.get(url_get_period)
    if response.status_code == 200:
        index_periods = response.json()
    else: 
        print(response.status_code)

    data_link["periods"] = []
    for index_period in index_periods:
        period_id = index_period["id"]
        if response.status_code == 200:
            index_periods = response.json()
        else:
            return response.status_code
        
        url_get_segment = f"http://taldau.stat.gov.kz/ru/Api/GetSegmentList?indexId={index_id}&periodId={period_id}"
        time.sleep(2)
        response = requests.get(url_get_segment)
        if response.status_code == 200:
            segment = response.json()
        else:
            return response.status_code
        dicid = segment[0]["dicId"]
        term_ids = segment[0]["termIds"]
        numbers_list = [num.strip() for num in dicid.split("+")]
        dic_ids = ",".join(numbers_list)


        url_get_index_period = f"http://taldau.stat.gov.kz/ru/Api/GetIndexPeriods?p_measure_id=1&p_index_id={index_id}&p_period_id={period_id}&p_terms={term_ids}&p_term_id=741880&p_dicIds={dic_ids}"
        time.sleep(2)
        response = requests.get(url_get_index_period)
        if response.status_code == 200:
            dates = response.json()
        else:
            return response.status_code
        
        
        period_info = {"period": index_period, "dic_ids": dic_ids, "term_ids": term_ids, "dates": dates}
        data_link["periods"].append(period_info)

    time.sleep(2)
    url_get_name = f"http://taldau.stat.gov.kz/ru/Api/GetIndexAttributes?periodId={data_link['periods'][0]['period']['id']}&measureID=1&measureKFC=1&indexId={index_id}"        
    response = requests.get(url_get_name)
    if response.status_code == 200:
        name = response.json()["name"]
    else:
        return response.status_code

    data_link["name"] = name
    return data_link

