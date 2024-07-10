import argparse

from src.resume_data_crawler import resume_data_crawler
from src.operation_detail_crawler import operation_detail_crawler
from src.resume_land_info_crawler import resume_land_info_crawler
from src.land_satellite_crawler import land_satellite_crawler


def main():
    parser = argparse.ArgumentParser(description='Run specified crawler')
    parser.add_argument('crawler', type=str, help='Name of the crawler to run (resume_data, operation_detail, resume_land_info, land_satellite)')
    args = parser.parse_args()
    
    switcher = {
        'resume_data': resume_data_crawler,
        'operation_detail': operation_detail_crawler,
        'resume_land_info': resume_land_info_crawler,
        'land_satellite': land_satellite_crawler
    }
    
    crawler = switcher.get(args.crawler)
    if not crawler:
        raise ValueError(f'Invalid crawler name: {args.crawler}')
    
    crawler()
    
    
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'Error: {e}')
        raise e