#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-source", choices=["local", "remote"], nargs='1', help="where data should be gotten from")
    args = parser.parse_args()
    
    location = args.source

    if location == "local":
        Load_Local()
    else:
        Load_Remote()
    
if __name__ == "__main__":
    main()

