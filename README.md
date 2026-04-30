# Globus Transfers

Directory dedicated to production scripts for facilitating Globus Transfers that are available to the wider Purdue community. There are a handful of scripts and several walkthroughs. 

For automation, Globus confidential clients are recommended. Globus provides a [Developer Guide](https://docs.globus.org/api/auth/developer-guide/) that describes how to develop apps and services using Globus Auth. Globus does a good job at explaining all of the options, but the main one that I'll cover in this repo is the confidential client. 

It's called a confidential client because there's a secret associated with the client ID (a UUID Globus assigns to each application). The secret can be hidden in a .env file, saved in a keyring, or if needed, hardcoded into a script for quick tests. This allows users to run things headlessly whereas a Globus native client requires manual input to confirm access. This means there are a few extra steps involved, but once it's set up, you're good to go.  

Ideally, this repository will be useful for folks at Purdue and beyond to implement Globus workflows to automate data transfer and facilitation. 
 
## Scripts
There are a few methods that can be used for retaining secrets. The main ones I've experimented with are Python's `keyring` library and a `.env` file.

The Python `keyring` library provides an easy way to store and retrieve passwords securely. The `keyring` library relies on your operating system's native credential manager (like Windows Credential Locker, macOS Keychain, or Linux Secret Service). These managers are designed to encrypt your secrets and automatically decrypt them.

The `.env` file is less secure, but can be broken down to prevent other users from reading it. A `.env` file can be better when the program is scheduled on a task scheduler (or cron job) because this job usually starts in a background, non-interactive state. Most of the computers I'm running on are Windows. These Window devices start Session 0, a headless state where the OS credential manager is typicaly locked. The `.env` file lives where the script is executed and it removes a potential point of failure when automating Globus scripts.


## Walkthroughs
This repository is dedicated to walkthroughs that are useful for setting up things for a confidential client and will evolve into other user guides for using the Globus command line utility, Flows, and Globus Compute. 