# EPFL - Financial Big Data Project - Autumn 2023

## Performance of Optimal Causal Thermal Path Based Trading Strategies

Final project of the course MATH-525 Financial Big Data (EPFL, Fall 2023).

our github repo : https://github.com/GeorgetteFR/thermal_optimal_path

All the data can be downloaded from internet and computed from the main.ipynb file 

## Installation and Requirements

The requirements are listed in the [requirements.txt](requirements.txt) file. To install them, run the following command in the root directory of the project:

```bash
pip install -r requirements.txt
```

To run the code of the project, execute the `main.ipynb` file with the data provided in the `data` folder.

## Brief description of the repository

```         
fin-big-data_project/
│
├── data/                                                      # Folder containing the data
│
├── data_loading/                                              # Code to load data from the web
│   ├── [web_loader.py](data_loading/web_loader.py)            # Functions to download data
│
├── processing/                                                # Code to augment data and compute lag metrics
│   ├── [lag_metrics.py](processing/lag_metrics.py)            # Code to compute lag metrics
│   ├── [augment_data.py](processing/augment_data.py)          # Code to augment the original data
│
├── thermal_optimal_path/                                      # Code to run the Optimal Causal Thermal Path algorithm
│   ├── [optimal_path.py](thermal_optimal_path/optimal_path.py) # Implementation of the algorithm
│
├── [helpers.py](helpers.py)                                   # Functions for running investment strategies
├── [strategies.py](strategies.py)                             # Trading strategies implementation
├── [main.ipynb](main.ipynb)                                   # Main notebook to run the entire project
│
├── [requirements.txt](requirements.txt)                       # Requirement file
│
└── README.md 
```

### Authors

-   Elliot Jacquet-Francillon
-   Antoine Merel

### Supervisors

-   [Damien Challet](https://people.epfl.ch/damien.challet)

Last edited: 2024-02-01
