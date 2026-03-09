from airflow.sdk import dag, task, task_group
import pathlib
from datetime import datetime

SCHEMA="scraped_quotes", 
DAG_ROOT=pathlib.Path(__file__).parent,
BUCKET='l3-external-storage-753900908173',
S3_KEYS={
    'extract': '{{ dag.dag_id }}/extract/{{ ds }}',
    }

@dag(
    schedule='@daily',
    start_date=datetime(2025, 3, 5),
    end_date=datetime(2026, 3, 13),
    )
def quotes_scraper():

    @task_group
    def extract():

        @task
        def quotes(filepath, extract_key, BUCKET):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from bs4 import BeautifulSoup
            
            html = pathlib.Path(filepath).read_text()
            
            hook = S3Hook()
            hook.load_string(
                string_data=html,
                key=extract_key + '/quotes.html',
                bucket_name=BUCKET,
                replace=True,
                )
            
            soup = BeautifulSoup(html)
            author_containers = soup.find_all('small', {'class': 'author'})
            author_links = [x.parent.find('a').attrs['href'] for x in author_containers]

            return author_links
        
        @task
        def authors(author_links, extract_key, BUCKET, ds):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook = S3Hook()
            for link in author_links:
                author_name = link.split('/')[-1]
                filepath = pathlib.Path(__file__).parent / (author_name + '-' + ds + '.html') 
                html = filepath.read_text()
                key = extract_key + f'/authors/{author_name}.html'
                hook.load_string(
                    string_data=html,
                    key=key,
                    bucket_name=BUCKET,
                    replace=True
                    )
        
        extract_key = S3_KEYS['extract']
        filepath = (DAG_ROOT / 'quotes' / 'quotes-{{ ds }}.html').as_posix()
        author_links = quotes(filepath, extract_key, BUCKET)
        authors(author_links, extract_key, BUCKET)

    @task_group
    def transform():

        @task
        def not_implemented():
            pass

    @task_group
    def load():

        @task
        def not_implemented():
            pass

        
    extract() 
    
quotes_scraper()
