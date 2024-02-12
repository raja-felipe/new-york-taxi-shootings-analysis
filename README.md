# Taxi and Shooting Data Analysis

**Research Goal:** My research goal is analysis of the relationship between taxi demands and shootings

**Timeline:** The timeline for the research area is 2022 - 2023.

To run the pipeline, please visit the `scripts` directory and run the files in order. 

1. `DataDownloader.py`: This downloads the landing data into the `data/landing` directory.
2. `DataStandardizer.py`: This downloads raw data, meaning column cased and data type casted data, into `data/raw` directory.
    - Also run `prelim_analysis_shootings.ipynb`, `prelim_analysis_green.ipynb`, and `prelim_analysis_green_2.ipynb` to see the visual aspects of the analysis
3. `DataCurator.py`: This downloads the preprocessed and one hot encoded data into `data/curated` directory.
4. `DataAggregator.py`: This aggregates the curated data in the `data/curated` directory.
5. `DataRenamer.py`: This renames columns in the aggregated data in the `data/curated` directory.
6. `DataMerger.py`: This merges the curated shooting and green taxi data in the `data/curated` directory.
7. `curated_analysis.ipynb`: This analyzes the visual trends in the curated data.
8. `DataModeller.py`: This trains, tunes, and tests an ARIMA and Random Forest Regression model. It saves the results of the tests in the `data/landing` directory.
    - Additionally, visual explanations and analysis of the results are contained in the `data_models_visualized.ipynb` notebook.
    - The script saves text files used by the notebook to output relevant metrics for said models.

Afterwards, in the `report` folder is a LaTeX/PDF file of the report associated with this analysis.

**NOTE**: You could either manually attach the root of the repository to the path of your IDEA (VS Code, PyCharm will have tutorials on this). Otherwise, to run the code:
- Open your  console and cd to the git repo directory using cd {path_to_repo}
- For each script writen below, run it as python3 -m scripts.{name_of_script}
