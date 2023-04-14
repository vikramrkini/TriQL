import React, { useState, useEffect } from 'react';
import './app.css';
import QueryBuilder from './QueryBuilder'; // Import QueryBuilder
import FileUploader from './FileUploader';

function FileListDropdown() {
  const [fileList, setFileList] = useState([]);
  const [selectedFile, setSelectedFile] = useState('');
  const [tables, setTables] = useState([]);
  const [selectedAttributes, setSelectedAttributes] = useState([]);

  useEffect(() => {
    fetchFileList();
  }, []);

  useEffect(() => {
    if (selectedFile !== '') {
      fetchTables();
    }
  }, [selectedFile]);

  const fetchFileList = async () => {
    try {
      const response = await fetch('http://127.0.0.1:5000/listschemas', {
        mode: 'cors',
        headers: {
          'Content-Type': 'application/json',
          Accept: 'application/json',
        },
      });
      const data = await response.json();
      setFileList(data.fileList);
    } catch (error) {
      console.error(error);
    }
  };

  const fetchTables = async () => {
    try {
      const response = await fetch(`http://127.0.0.1:5000/get_table_attrs/${selectedFile}.db`, {
        mode: 'cors',
        headers: {
          'Content-Type': 'application/json',
          Accept: 'application/json',
        },
      });
      const data = await response.json();
      if (data) {
        const tables = Object.entries(data).map(([tableName, columnNames]) => ({
          tableName,
          columnNames: columnNames.map((columnName) => ({
            name: columnName,
            checked: false,
          })),
        }));
        setTables(tables);
      }
    } catch (error) {
      console.error(error);
    }
  };

  const handleFileSelect = (event) => {
    setSelectedFile(event.target.value);
  };

  // const handleAttributeSelect = (event, tableName, attributeName) => {
  //   const isChecked = event.target.checked;
  //   setTables((prevTables) =>
  //     prevTables.map((table) =>
  //       table.tableName === tableName
  //         ? {
  //             ...table,
  //             columnNames: table.columnNames.map((column) =>
  //               column.name === attributeName ? { ...column, checked: isChecked } : column
  //             ),
  //           }
  //         : table
  //     )
  //   );
  //   setSelectedAttributes((prevSelectedAttributes) =>
  //     isChecked ? [...prevSelectedAttributes, { tableName, attributeName }] : prevSelectedAttributes.filter((attr) => attr.attributeName !== attributeName)
  //   );
  // };
  const handleAttributeSelect = (event, tableName, attributeName) => {
    const updatedTables = tables.map((table) => {
      if (table.tableName === tableName) {
        const updatedColumns = table.columnNames.map((column) => {
          if (column.name === attributeName) {
            return { ...column, checked: event.target.checked };
          }
          return column;
        });
        return { ...table, columnNames: updatedColumns };
      }
      return table;
    });
    setTables(updatedTables);
  };
  
  return (
    <div className="app">
      <div className="sidebar">
        <FileUploader />
        <h1>Select Schema:</h1>
        <select class = "custom-dropdown" value={selectedFile} onChange={handleFileSelect}>
          {fileList.map((fileName, index) => (
            <option class = "custom-dropdown" key={index} value={fileName}>
              {fileName}
            </option>
          ))}
        </select>
        <h1>Entities:</h1>
        {tables.length > 0 ? (
          tables.map((table) => (
            <div key={table.tableName}>
              <h2>{table.tableName}</h2>
              <ul>
                {table.columnNames.map((column) => (
                  <li key={column.name}>
                    <input
                      type="checkbox"
                      id={`${table.tableName}-${column.name}`}
                      name={column.name}
                      value={column.name}
                      checked={column.checked}
                      onChange={(event) => handleAttributeSelect(event, table.tableName, column.name)}
                    />
                    <label htmlFor={`${table.tableName}-${column.name}`}>{column.name}</label>
                  </li>
                ))}
              </ul>
            </div>
          ))
        ) : (
          <p>No attributes found for selected file.</p>
        )}
      </div>
      <div className="main">
      <QueryBuilder
        selectedAttributes={tables
          .map((table) =>
            table.columnNames
              .filter((column) => column.checked)
              .map((column) => ({
                tableName: table.tableName,
                attributeName: column.name,
              }))
          )
          .flat()}
          schemaName={selectedFile}
      />
      </div>
    </div>
  );
  }
export default FileListDropdown; 

