#!/usr/bin/env python
# coding: utf-8

# In[2]:


from definitions import *


# In[10]:


import argparse

def main():
    parser = argparse.ArgumentParser(description = "load in data for final project")
    parser.add_argument("-source", choices=["local", "remote"], nargs='+', help="where data should be gotten from")
    args = parser.parse_args()
    
    location = args.source

    if location == "local":
        Load_Local()
    else:
        Load_Remote()
    
if __name__ == "__main__":
    main()


# In[ ]:




