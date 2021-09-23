import React, { useState } from 'react';
import Cardinput from './component/Cardinput';
import ShowOutput from './component/Showoutput';
import Submit from './component/Submit';
import './App.css';

function App() {
  const [submit, setSubmit] = useState(false);        // Variable for checking the status of the button. 
  const [onSearch,setOnsearch]=useState(false);       //
  const [output, setOutput] = useState([]);           // List to store output data.
  const [list, setList] = useState([]);               // List to store input data.
  const [listticker, setlistTicker] = useState([]);   // List to store list of tickers.
  const [clonelist,setClonelist] = useState([]);
  return (
    <div className = "background-page">
      <div className = "Input">
          <Cardinput submit={submit} setSubmit={setSubmit} list={list} setList = {setList}/>  
          <Submit submit = {submit} setOutput = {setOutput} list = {list} clonelist = {clonelist} setClonelist = {setClonelist} setOnsearch={setOnsearch}/>  
      </div>
      <div className = "Output">
        <ShowOutput output={output} setOutput={setOutput} list={list} setList={setList} clonelist={clonelist} onSearch={onSearch} setOnsearch={setOnsearch}/>
      </div> 
    </div>
  )
}
export default App;