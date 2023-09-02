import re

from mftool import Mftool


#
# sys.path.insert(0, r'')
#
# # set logging
# logging.config.fileConfig(r"conf/logging.conf")
# logger = logging.getLogger(__name__)
#
# logger.info("setting sys path for src.zip file")
#
# if os.path.exists('python.zip'):
#     sys.path.insert(0, 'python.zip')
# else:
#     sys.path.insert(0, './python')


def run_pipeline():
    mf = Mftool()
    print(mf.get_today())  # dates
    print(mf.get_friday())

    # js = json.loads(mf.get_all_amc_profiles())
    quant = mf.get_available_schemes("QUANT")
    icici = mf.get_available_schemes("ICICI")
    hdfc = mf.get_available_schemes("HDFC")

    small_cap_funds = dict()
    [small_cap_funds.update({k: quant[k]}) for k in quant if re.findall('small cap|smallcap', quant[k], re.IGNORECASE)
     and re.findall('direct plan|directplan', quant[k], re.IGNORECASE)]

    [small_cap_funds.update({k: icici[k]}) for k in icici if re.findall('small cap|smallcap', icici[k], re.IGNORECASE)
     and re.findall('direct plan|directplan', icici[k], re.IGNORECASE)]

    [small_cap_funds.update({k: hdfc[k]}) for k in hdfc if re.findall('small cap|smallcap', hdfc[k], re.IGNORECASE)
     and re.findall('direct plan|directplan', hdfc[k], re.IGNORECASE)]

    print(small_cap_funds)

    # df1=mf.get_scheme_historical_nav(code='120828')
    print(mf.get_scheme_quote(code=120828)) #{'scheme_code': '120828', 'scheme_name': 'quant Small Cap Fund - Growth Option - Direct Plan', 'last_updated': '01-Sep-2023', 'nav': '195.7574'}


    # for k in small_cap_funds:
    #     print(k,mf.history(k,start=None,end=None,period='3mo',as_dataframe=True))


    # nav_data = mf.get_scheme_historical_nav_for_dates('119551', '01-01-2021', '11-05-2021')
    # print(nav_data)

if __name__ == '__main__':
    print("*" * 20)
    run_pipeline()
    print("*" * 20)
    import yfinance as yf

    tickers = yf.Tickers('msft aapl goog')
    print(tickers.tickers['MSFT'].info)

