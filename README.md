# FMERepoTran
- Development utility tool to transfer Repositories between FME Servers.

# Run Environment
- Python version is v3.8 or above
- Python.exe main.py [arg]
  - copy: copy repositories
  - compare: compare repositories

app.jon
  const definetion for the application.
  
CallAPI.py
  The interal Python class calls FME Server API using request.
  
FMEAPI.py
  The Python class defines the API method:
    check_health: check FME Server health.
    list_repos: list repositories.
    list_repo_sub_items: list sub-items of a specific repo.
