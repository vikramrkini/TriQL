import React, { useState } from 'react';
import './app.css';

const QueryBuilder = ({ selectedAttributes = [], schemaName }) => {
  const [datalogQuery, setDatalogQuery] = useState('');
  const [conditions, setConditions] = useState([]);
  const [sqlQuery, setSqlQuery] = useState('');
  const [mongoQuery, setMongoQuery] = useState('');
  const [checkedAttributes, setCheckedAttributes] = useState([]);

  const handleCheckboxChange = (tableName, attributeName, isChecked) => {
    if (isChecked) {
      setCheckedAttributes((prev) => [...prev, `${tableName}.${attributeName}`]);
    } else {
      setCheckedAttributes((prev) => prev.filter((attr) => attr !== `${tableName}.${attributeName}`));
    }
  };

  const handleOperatorChange = (tableName, attributeName, operator) => {
    setConditions((prevConditions) => {
      const updatedConditions = prevConditions.filter(
        (condition) => condition.tableName !== tableName || condition.attributeName !== attributeName
      );
      if (operator !== 'None') {
        updatedConditions.push({ tableName, attributeName, operator, value: '' });
      }
      return updatedConditions;
    });
  };

  const handleValueChange = (tableName, attributeName, value) => {
    setConditions((prevConditions) =>
      prevConditions.map((condition) =>
        condition.tableName === tableName && condition.attributeName === attributeName
          ? { ...condition, value }
          : condition
      )
    );
  };
  const handleShowChange = (tableName, attributeName, isChecked) => {
    setCheckedAttributes((prevCheckedAttributes) => {
      const updatedCheckedAttributes = new Set(prevCheckedAttributes);
      const attributeString = `${tableName}.${attributeName}`;
      if (isChecked) {
        updatedCheckedAttributes.add(attributeString);
      } else {
        updatedCheckedAttributes.delete(attributeString);
      }
      return updatedCheckedAttributes;
    });
  };
  // const handleSubmit = async (e) => {
  //   e.preventDefault();
  
  //   const tables = Array.from(new Set(selectedAttributes.map((attr) => attr.tableName)));
  //   const attributes = Array.from(checkedAttributes);
  //   const formattedConditions = conditions.map(
  //     (condition) => `${condition.tableName}.${condition.attributeName} ${condition.operator} ${condition.value}`
  //   );

  //   const response = await fetch('http://127.0.0.1:5000/generate_datalog', {
  //     method: 'POST',
  //     headers: {
  //       'Content-Type': 'application/json',
  //     },
  //     body: JSON.stringify({ schemaName, tables, attributes, conditions: formattedConditions }),
  //   });

  //   const data = await response.json();
  //   setDatalogQuery(data.query);
  // };
  const handleSubmit = async (e) => {
    e.preventDefault();
  
    const tables = Array.from(new Set(selectedAttributes.map((attr) => attr.tableName)));
    const attributes = Array.from(checkedAttributes);
    const formattedConditions = conditions.map(
      (condition) => `${condition.tableName}.${condition.attributeName} ${condition.operator} ${condition.value}`
    );
  
    // Fetch Datalog query
    const datalogResponse = await fetch('http://127.0.0.1:5000/generate_datalog', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ schemaName, tables, attributes, conditions: formattedConditions }),
    });
  
    const datalogData = await datalogResponse.json();
    setDatalogQuery(datalogData.query);
  
    // Fetch SQL query
    const sqlResponse = await fetch('http://127.0.0.1:5000/datalog_to_sql', { // Update the endpoint here
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query: datalogData.query, tables }), // Pass the datalogQuery as 'query' and tables in the request body
    });
    
    const sqlData = await sqlResponse.json();
    console.log("SQL Data:", sqlData);
    setSqlQuery(sqlData.SQL_query);

    // Fetch MongoDB query
    const mongoResponse = await fetch('http://127.0.0.1:5000/datalog_to_mongo', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query: datalogData.query, tables }),
  });
    const mongoData = await mongoResponse.json();
    setMongoQuery(mongoData.mongo_query);
  };
  
  return (
    <div className="query-builder">
      <h2>Query Builder</h2>
      <div className="selected-attributes">
        <h3>Selected Attributes:</h3>
        <table>
          <thead>
            <tr>
              <th>Attribute</th>
              <th>Show</th>
              <th>Operator</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            {selectedAttributes.length > 0 ? (
              selectedAttributes.map((attribute) => (
                <tr key={`${attribute.tableName}-${attribute.attributeName}`}>
                  <td>
                    {attribute.tableName}.{attribute.attributeName}
                  </td>
                  <td>
                    <input
                    type="checkbox"
                    onChange={(e) =>
                    // handleShowChange(attribute.tableName, attribute.attributeName, e.target.checked)
                    handleCheckboxChange(attribute.tableName, attribute.attributeName, e.target.checked)
                     } />
                  </td>
                  <td>
                    <select
                      onChange={(e) =>
                        handleOperatorChange(attribute.tableName, attribute.attributeName, e.target.value)
                      }
                    >
                      <option value="None">None</option>
                      <option value="=">=</option>
                      <option value="!=">!=</option>
                      <option value="<">&lt;</option>
                      <option value="<=">&lt;=</option>
                      <option value=">">&gt;</option>
                      <option value=">=">&gt;=</option>
                    </select>
                  </td>
                  <td>
                    <input
                      type="text"
                      onChange={(e) =>
                        handleValueChange(attribute.tableName, attribute.attributeName, e.target.value)
                      }
                    />
                  </td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan="4">No attributes selected.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <form onSubmit={handleSubmit}>
        <button type="submit" className="query-builder__button">
          Generate Query
        </button>
      </form>
      <div className="generated-query">
        
        <div className="generated-query__container">
        <h3>Generated Datalog Query:</h3>
        <pre>{datalogQuery}</pre>
        </div>
        <div className="generated-query__container">
          <h3>Generated SQL Query:</h3>
          {sqlQuery}
        </div>
        <div className="generated-query__container">
          <h3>Generated MongoDB Query:</h3>
          {mongoQuery}
        </div>
      </div>
     </div>
  );
  
};

export default QueryBuilder;
