# UtilityFunctions
This is a repo of commonly used functions, copied from an enterprise account.

## Files and Functions In Each
### aws_db_details
Contains the `AwsClusterSecretsMap` class, which can be used to make AWS connections by cluster nickname (as started by Robert Shaffer in sm_utils). To connect using a cluster nickname, use `connect_to_db_with_psycopg2_nickname` and `connect_to_db_with_sqlalchemy_nickname`.

### command_line_utils
Function to allow you to enter a bash command through Python and store the output

### connection_utils
Contains `AwsDatabaseConnectionManager` and `LastpassManager` classes, as well as utility functions for connecting to AWS services. 

### database_utils
Contains functions for tasks commonly performed when connected to a database. 

### file_utils
Contains functions for tasks commonly performed with files and directories. 

### log_config
Contains all necessary information for configuring a logger. To configure a logger in a specific file add this near the top of the file:
```{python}
from log_config import get_logger

log = get_logger(__name__)
```

### queries_as_functions
Contains commonly used queries, stored as functions. The output of the functions are Psycopg2 SQL objects. 

### s3_utils
Contains functions to perform common tasks when connecting to S3. 

### user_input_utils
Contains functions for common checks one must perform when asking for user input when running a Python file. 

## To Install as a Package in another Repo

1. Ensure you have your EUA ID stored as an environment variable, `${EUA_ID}`. 

### Pre-Setup 
**Note:** This should only have to be performed once per machine.  

If you have not already done so... Add your EUA_ID to your bash_profile, so the environment variables below automatically get substituted. 

#### Step 1:
In your root, open `.bash_profile` in your text editor of choice.

#### Step 2: 
Add the following line to your `.bash_profile`, substituting {EUA_ID} with your personal EUA:
```bash
export EUA_ID="{EUA_ID}"
```

Eg.:
```bash
export EUA_ID="BOXN"
```

#### Step 4: 
Save the changes and restart terminal. 
1) Save the `.bash_profile`. 
2) Close your text editor.
3) Restart the terminal
4) Source your `.bash_profile`

```bash
source .bash_profile
```

#### Step 5:
Ensure the changes have saved to your enviroment. Run:
```bash
echo ${EUA_ID} 
```
If your personal EUA is printed back to you, you're good to proceed. 

2. Use pip to install the package from git. 
```{python}
pip install git+https://${EUA_ID}@github.cms.gov/CMS-MAX/msp-library.git
```
3. The package can now be used by importing the individual files or functions from `msp_library`

**Ex:**
```{python}
from msp_library.log_config import get_logger
```
