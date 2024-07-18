# raw-crawler-taft
## 介紹
爬取產銷履歷資料，包含農地衛星數據、生產作業情況、產銷履歷資料、農地資訊等。

## 專案架構
```
.
|--main.py                  # Main entry point
|--config.py                # Environment file loading 
|--requirements.txt         # required packages
|--config.ini               # environment variables for all environment
|--poetry.lock              # poetry lock file
|--pyproject.toml           # poetry project file
|
|--src                      # Crawlers
|   |--land_satellite_crawler.py        # 爬取農地衛星數據
|   |--operation_detail_crawler.py      # 爬取生產作業情況
|   |--resume_data_crawler.py           # 爬取產銷履歷資料
|   |--resume_land_infor_crawler.py     # 爬取農地資訊


```

## 開發環境建置
### 啟動虛擬環境
```
# venv
source {your_venv_folder_name}/bin/activate

# conda
conda activate {your_env_name}
```

### 在虛擬環境下載需要套件 (已載過可跳過)

```
cd {project_path}
```
```
pip install -r requirements.txt
```

### 執行

  使用main.py作為進入點, 加上參數執行
```
# List of available crawlers:
    - land_satellite_crawler
    - operation_detail_crawler
    - resume_data_crawler
    - resume_land_infor_crawler

python main.py {crawler_name}
```
