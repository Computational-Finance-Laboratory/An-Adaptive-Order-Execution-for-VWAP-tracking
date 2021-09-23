import React, { useState, useEffect } from 'react';
import CalendarModule from './CalendarModule';
import DataInput from './DataInput';
import AddList from './AddList';
import { Card } from 'react-bootstrap';
import "./style.css";

const Cardinput = ({ setSubmit, list, setList }) => {

    const [lengthlist, setLengthList] = useState(true);         // Variable for checking the status of the list .If the number in the list of 50 more, status will be False and can't add more items.
    const [datestatus, setDatestatus] = useState(false);        // Variable for checking the status of the date .If a date has not been selected , status will be False and can't input volume,time and can't click add button.
    const [listTicker, setListticker] = useState([]);           // array ที่รับ ticker มาจาก backend.
    const [datestate, setDate] = useState(new Date());          // Initial date to the present.
    const len=Object.keys(list).length;                         // length of list (int).
    
    useEffect(() => {                                           //If list not have data can't submit.
        setSubmit(len>0);
    },[list]);

    return(
        <Card className ="cardinput">
            <CalendarModule 
                datestate={datestate}
                setDate={setDate}
                setlist = {setList}
                setdatestatus={setDatestatus}
                listTicker= {listTicker}
                setlistticker={setListticker}
            />
            <div className = "card-add-input">
                <AddList  
                    datestate={datestate}
                    list = {list} 
                    setlist = {setList}
                    datestatus = {datestatus}
                    listticker = {listTicker}
                    lengthlist = {lengthlist}
                    setLengthList = {setLengthList}
                />
            </div>
            <div className = "card-data-out">
                <div className = "input-header">
                    {list.length>0? 
                        <div>
                        <strong className = "input-header-ticker">Ticker</strong>
                        <strong className = "input-header-volume">Volume</strong>
                        <strong className = "input-header-time-start">From</strong>
                        <strong className = "input-header-time-end">To</strong>
                        </div>:
                        <div></div>
                    }
                </div>
                <div className = "card-data">
                    <DataInput 
                        list={list} 
                        setlist = {setList}
                        setLengthList = {setLengthList}
                    />
                </div>
            </div>
        </Card>
    )
}

export default Cardinput;