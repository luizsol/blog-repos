import datetime
from decimal import Decimal
import requests
from urllib.parse import urlencode

from bs4 import BeautifulSoup

BCB_CDI_URL = 'https://www3.bcb.gov.br/CALCIDADAO/publico/' \
              'corrigirPeloCDI.do?method=corrigirPeloCDI'

BCB_FIRST_CDI_DATE = datetime.date(1986, 3, 6)
headers = {'content-type': 'application/x-www-form-urlencoded'}


def fetch_cdi_for_date_interval(start_date=None, end_date=None):
    if start_date is None:
        start_date = BCB_FIRST_CDI_DATE

    if end_date is None:
        end_date = (datetime.date.today() - datetime.timedelta(-1))

    payload = {
        'aba': 5,
        'dataInicial': start_date.strftime('%d/%m/%Y'),
        'dataFinal': end_date.strftime('%d/%m/%Y'),
        'valorCorrecao': '1000000,00',
        'percentualCorrecao': '100,00',
    }

    result = requests.post(
        BCB_CDI_URL, data=urlencode(payload), headers=headers)

    if result.ok:
        soup = BeautifulSoup(result.text, 'html.parser')
        error_div = soup.findAll("div", {"class": "msgErro"})

        if len(error_div) > 0:
            raise RuntimeError(f'CDI not available: {error_div}')

        data = soup.findAll('td', {'class': 'fundoPadraoAClaro3'})
        data = data[9]
        data = data.contents[0]
        return Decimal(data.replace('.', '').replace(',', '.'))

    else:
        raise RuntimeError('Unable to retrieve CDI value.')


def fetch_daily_cdi_for_date_range(start_date=None, end_date=None):
    if start_date is None:
        start_date = BCB_FIRST_CDI_DATE

    if end_date is None:
        end_date = (datetime.date.today() - datetime.timedelta(-1))

    previous_date = start_date
    current_date = start_date + datetime.timedelta(days=1)
    res = {}

    while current_date <= end_date:
        try:
            res[current_date] = fetch_cdi_for_date_interval(
                start_date=previous_date, end_date=current_date)

            print(f'{current_date}: {res[current_date]}')

            previous_date = current_date
            current_date += datetime.timedelta(days=1)
        except RuntimeError:
            current_date += datetime.timedelta(days=1)

    return [{'date': key, 'cdi': value} for key, value in res.items()]
