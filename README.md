# Globus Transfers

Directory dedicated to production scripts for facilitating Globus Transfers that are available to the wider Purdue community. There are a handful of scripts and several walkthroughs. 

For automation, Globus confidential clients are recommended. Globus provides a Globus [Developer Guide](https://docs.globus.org/api/auth/developer-guide/) that describes how to develop apps and services using Globus Auth. Globus does a good job at explaining all of the options, but the main one that I'll cover in this repo is the confidential client. 

It's called a confidential client because there's a secret associated with the client ID (a UUID Globus assigns to each application). The secret can be hidden in a .env file, saved in a keyring, or if needed, hardcoded into a script for quick tests. This allows users to run things headlessly whereas a Globus native client requires manual input to confirm access. This means there are a few extra steps involved, but once it's set up, you're good to go.  

 
## Scripts
There are a few methods that can be used for retaining secrets. The main ones I've experimented with are Python's `keyring` library and a `.env` file.


## Walkthroughs
