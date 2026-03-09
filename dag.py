from airflow.sdk import dag, task, task_group
import pathlib
from datetime import datetime

SCHEMA="scraped_quotes"
DAG_ROOT=pathlib.Path(__file__).parent
BUCKET="l3-external-storage-753900908173"
S3_KEYS={
    'extract': '{{ dag.dag_id }}/extract/{{ ds }}'
    }

@dag(
    schedule='@daily',
    start_date=datetime(2026, 3, 8),
    end_date=datetime(2026, 3, 16),
    )
def quotes_scraper():

    @task_group
    def extract():

        @task
        def quotes(filepath, extract_key):
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

            soup = BeautifulSoup(html, features='lxml')
            author_containers = soup.find_all('small', {'class': 'author'})
            author_urls = [x.parent.find('a').attrs['href'] for x in author_containers]

            return author_urls
        
        @task
        def authors(author_links, extract_key, ds):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook = S3Hook()

            for link in author_links:

                author_name = link.split('/')[-1]

                filepath = (
                    DAG_ROOT / 
                    'authors' /
                    (ds + '-' + author_name + '.html')
                )

                html = filepath.read_text()

                key = extract_key + f'/authors/{author_name}.html'

                hook.load_string(
                    string_data=html,
                    key=key,
                    bucket_name=BUCKET,
                    replace=True
                )

        # Define the filepath for the quotes html file
        filepath = (DAG_ROOT / 'quotes' / 'quotes-{{ ds }}.html').as_posix()

        # Call the `quotes` task
        author_links = quotes(filepath, S3_KEYS['extract'])

        # Call the `authors` task
        authors(author_links, S3_KEYS['extract'])

    @task_group
    def transform():

        @task
        def not_implemented():
            pass

        not_implemented()

    @task_group
    def load():

        @task
        def not_implemented():
            pass

        not_implemented()

        
    extract() >> transform() >> load()
    
quotes_scraper()