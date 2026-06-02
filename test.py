from neurodb_python_driver import NeuroDBDriver
driver = NeuroDBDriver('127.0.0.1',8839)
result=driver.executeQuery('match (n)-[r]->(m) return n,r,m')
print(result.__dict__)
