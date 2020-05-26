import sys 
import os 
path  = "C:\\Users\\parzival\\Documents\\Accenture\\covid19"  
clone = "git clone -v https://github.com/thepanacealab/covid19_twitter.git"   

# os.system("sshpass -p your_password ssh user_name@your_localhost") 
os.chdir(path) 
# Specifying the path where the cloned project needs to be copied 
os.system(clone) # Cloning
# import git

# git.Repo.clone_from("https://github.com/thepanacealab/covid19_twitter.git", "C:\\Users\\parzival\\Documents\\Accenture\\covid19_twitter")

