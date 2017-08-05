# EEG_Server

Web server built using Spring Boot that creates EEG data analysis jobs, check their status and get the result over a REST API.

##### The server listens for requests:  
- /jobs/submit/(id)?{qp}  
		- to submit a job with configuration specified in the query parameters  
- /jobs/check/(id)  
		- to check the status of a job with id=={id} -> either RUNNING OR FINISHED  
- /jobs/result/(id)  
		- to get the result output of a job with id=={id}
- /jobs/log/{id}  
		- to get the log of a job with id=={id}  
- /jobs/cancel/{id}    
		- to cancel a job with id=={id}
- /jobs/configuration/{name}  
		- to get the configuration of a job with id=={id}
- /classifiers/list  
		- to list all the saved classifiers
		

where the {id} or {name} are path variables provided by the client,
and {qp} are query parameters that configure the spark application.



##### The supported query parameters are:  

 0. Id (REQUIRED - the id of the job)  
  - id = {some_integer}*
 
 1. Input file (REQUIRED - choose what type of input you want to provide)
  - info_file={*path_to_file}  
 		OR  
  - eeg_file={*path_to_file} AND guessed_num={*number}  
 	
 2. Feature extraction (REQUIRED - choose the method used for classification)
  - fe = {dwt-8} 
 
 3. Classification  
  - a) (REQUIRED - choose whether to train or load a classifier)    
 	train_clf = {svm,dt,logreg,rf}   
 			OR  
 	load_clf={svm,dt,logreg,rf} AND load_name={src/main/resources/Classifiers/(*name*) }  
 		
  - b) (OPTIONAL - choose whether to save the classifier and give him a name)  
 	save_clf={true,false}   
 		AND  
 	save_name={*name}     
 
  - c) (REQUIRED - the specific configuration for each classifier)  
 	config_*clf_param*
 	-> can't be condensed as it's very specific for each classifier 
 
 4. Saving the results (REQUIRED - choose where to save the classifier performance)  
  - result_path={*path_to_file}     
