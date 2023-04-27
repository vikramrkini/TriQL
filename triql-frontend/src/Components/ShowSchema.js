import React, { useState, useEffect } from 'react';

function SchemaDisplay({ schemaName }) {
  const [schema, setSchema] = useState(null);

  useEffect(() => {
    if (schemaName) {
      fetchSchema();
    }
  }, [schemaName]);

  const fetchSchema = async () => {
    try {
      const response = await fetch(`http://127.0.0.1:5000/show_schema/${schemaName}`, {
        mode: 'cors',
        headers: {
          'Content-Type': 'application/json',
          Accept: 'application/json',
        },
      });
      const data = await response.json();
      setSchema(data);
    } catch (error) {
      console.error(error);
    }
  };

  return (
    <div>
      {schema && (
        <pre>
          {JSON.stringify(schema, null, 2)}
        </pre>
      )}
    </div>
  );
}

export default SchemaDisplay;
