import oracledb
import pandas as pd

# Configuración de la conexión a Oracle
DB_CONFIG = {
    'user': 'hr',
    'password': 'hr',
    'dsn': 'localhost:1521/orcl'  # Cambia 'xe' si el servicio es diferente
}

# Consultas SQL automatizadas como lista de cadenas
QUERIES = [
    # 1. Proyectar el nombre, apellido y salario de todos los empleados
    "SELECT first_name, last_name, salary FROM employees",

    # 2. Proyectar el código y nombre de los departamentos
    "SELECT department_id, department_name FROM departments",

    # 3. Seleccionar empleados con salario superior a 10,000
    "SELECT first_name, last_name, salary FROM employees WHERE salary > 10000",

    # 4. Ordenar empleados por salario (ascendente y descendente)
    "SELECT first_name, last_name, salary FROM employees ORDER BY salary ASC",
    "SELECT first_name, last_name, salary FROM employees ORDER BY salary DESC",

    # 5. Mostrar empleados cuyo nombre inicie por la letra L
    "SELECT first_name, last_name FROM employees WHERE first_name LIKE 'L%'",

    # 6. Consultar la cantidad de empleados en la base de datos
    "SELECT COUNT(*) AS total_employees FROM employees",

    # 7. Consultar la cantidad de empleados por cada departamento
    """
    SELECT department_id, COUNT(*) AS employee_count 
    FROM employees 
    GROUP BY department_id
    """,

    # 8. Empleado con el mayor salario de la organización
    """
    SELECT first_name, last_name, salary 
    FROM employees 
    WHERE salary = (SELECT MAX(salary) FROM employees)
    """,

    # 9. Promedio del salario de los empleados
    "SELECT AVG(salary) AS average_salary FROM employees",

    # 10. Promedio del salario de los empleados por departamento
    """
    SELECT department_id, AVG(salary) AS average_salary 
    FROM employees 
    GROUP BY department_id
    """,

    # 11. Cantidad de empleados que iniciaron a trabajar por mes
    """
    SELECT TO_CHAR(hire_date, 'Month') AS month, COUNT(*) AS employee_count 
    FROM employees 
    GROUP BY TO_CHAR(hire_date, 'Month')
    """,

    # 12. Empleados cuyo nombre inicia con 'st'
    "SELECT first_name, last_name FROM employees WHERE LOWER(first_name) LIKE 'st%'",

    # 13. Empleados cuyo nombre termina en la letra 's'
    "SELECT first_name, last_name FROM employees WHERE LOWER(first_name) LIKE '%s'",

    # 14. Empleados cuyo nombre contiene la cadena 'mar'
    "SELECT first_name, last_name FROM employees WHERE LOWER(first_name) LIKE '%mar%'",

    # 15. Empleados donde el nombre contiene vocal i o u y también la consonante s
    """
    SELECT first_name, last_name 
    FROM employees 
    WHERE REGEXP_LIKE(first_name, '.*[iu].*s.*', 'i')
    """
]

def execute_query(query):
    """Ejecuta una consulta SQL y devuelve los resultados como un DataFrame."""
    with oracledb.connect(**DB_CONFIG) as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]  # Nombres de columnas
        rows = cursor.fetchall()  # Filas
        return pd.DataFrame(rows, columns=columns)

def main():
    """Ejecuta todas las consultas y las almacena en una lista de DataFrames."""
    dataframes = []
    for i, query in enumerate(QUERIES):
        print(f"\nEjecutando consulta {i + 1}:")
        try:
            df = execute_query(query)
            dataframes.append(df)
            print(df)
        except Exception as e:
            print(f"Error ejecutando la consulta {i + 1}: {e}")
            dataframes.append(None)  # Si falla, añade None para mantener el orden

if __name__ == "__main__":
    main()
