# EEG_Server

Web server built using Spring Boot that accepts requests to create EEG data analysis jobs, check their status and get the result.

The server listens for requests:  
-> /jobs/submit/(id)?{qp}  
-> /jobs/check/(id)  
-> /jobs/result/(id)  
-> /jobs/log/{id}  
-> /jobs/cancel/{id}    
-> /jobs/configuration/{name}  
-> /classifiers/list  

where the {id} or {name} are path variables provided by the client,
and {qp} are query parameters that configure the spark application.


The supported query parameters are:  

1. Input file (REQUIRED)  
- info_file={*path_to_file}  
		OR   
- eeg_file={*path_to_file} AND guessed_num={*number}  
	  
	  
2. Feature extraction (REQUIRED)  
- fe = {dwt-8}  

3. Classification    
- a) (REQUIRED)   
	train_clf = {svm,dt,logreg,rf}   
			OR  
	load_clf={svm,dt,logreg,rf} AND load_name={src/main/resources/Classifiers/(*name*) }  
		 
- b) (OPTIONAL)  
	save_clf={true,false}   
		AND  
	save_name={*name}    

- c) (REQUIRED)  
	config_*clf_param*   
	-> can't be condensed as it's very specific for each classifier  
  
4. Saving the results (REQUIRED)  
- result_path={*path_to_file}     
