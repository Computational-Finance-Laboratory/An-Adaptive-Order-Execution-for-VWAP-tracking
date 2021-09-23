import React,{ useState } from 'react';
import TickerDropdown from './TickerDropdown';
import Switch from 'react-switch';
import TimeInput from 'react-time-input';
import './style.css';
import './style2.css';

const AddList = ({ datestate, list, setlist, datestatus, listticker, lengthlist, setLengthList }) => {
    // Set state element
    const [ticker,setTicker] = useState('')
    const [vol,setVolume] = useState('')
    const [start_time,setTimestart] = useState('')
    const [end_time,setTimestop] = useState('')
    const [id,setId] = useState(1)
    const date = datestate.getFullYear() + "-" + (datestate.getMonth() + 1) + "-" + datestate.getDate()
    // Variable that checks the number of ticker lists. If LengthListTicker = 0 can't click add button.
    const LengthListTicker=Object.keys(listticker).length
    // Set status for check a switch    
    const [check,setStatus]=useState(false)                    
    const [side,setType] = useState('Buy')                        
    const handleType = () =>{
        setStatus(!check);
        if (check) {
            setType('Buy')
        } 
        else {
            setType('Sell')
        }
    }
    const [ticker_match,setTickerMatch] = useState(false)
    const [timevalid,setTimeValid] = useState(false)

    // Change state element 
    function changevolume(e){
        const re = /^[0-9\b]+$/;
        if (e.target.value === '' || re.test(e.target.value)) {
            setVolume(e.target.value)
        }
    }
    function changeTimestart(val){
        // Check time format
        if(val.match("^([0-9]|0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$") != null){
            setTimeValid(true)
            console.log('match',val)
        }else{
            setTimeValid(false)
            console.log(val)
        }
        // Set Time start = value
        if(end_time === ''){
            setTimestart(val)
        }
        else if(val > end_time)
        {
            console.log(val,'>',end_time)
            alert('time is not valid')
            setTimeValid(true)
            setTimestart(val)
        }else{
            setTimeValid(false)
            setTimestart(val)
        }
    }
    function changeTimestop(val){
        // Check time format
        if(val.match("^([0-9]|0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$") != null){
            setTimeValid(true)
            console.log('match',val)
        }else{
            setTimeValid(false)
            console.log(val)
        }
        // Set Time end = value
        if(val < start_time)
        {
            console.log(val,'<',start_time)
            alert('time is not valid')
            setTimeValid(true)
            setTimestop(val)
        }else{
            setTimeValid(false)
            setTimestop(val)
        }
    }
    // Add element to list 
    function handleAdd(e) {
        // The list is limited to 50 items.
        const len=Object.keys(list).length
        if(len < 50){
            const volume=parseInt(vol).toLocaleString()
            setLengthList(true)
            setId(prevID => prevID + 1)
            const newList = list.concat({ id, ticker, volume, start_time, end_time,side,date});
            setlist(newList)
            setTimestart('')
            setTimestop('')
            e.preventDefault();
        }
        else{
            setLengthList(false)
        }
    }

    return(
        <div >
            <div className="input-dropdown"> <TickerDropdown setticker={setTicker} listticker={listticker} ticker_match={ticker_match} setTickerMatch={setTickerMatch}/> </div>
            <input className="input-volomn" type = "text"  value={vol} placeholder="Volume" disabled={!datestatus} onChange={changevolume}/>
            <TimeInput
                initTime=''
                placeholder='00:00'
                className='input-timestart'
                disabled={!datestatus}
                onTimeChange={changeTimestart}
   		    />
            <TimeInput
                initTime=''
                placeholder='00:00'
                className='input-timeend'
                disabled={!datestatus}
                onTimeChange={changeTimestop}
   		    />
                <Switch
                    onChange={handleType}
                    checked={check}
                    disabled={!datestatus}
                    onColor="#E4E5E5"
                    offColor="#E4E5E5"
                    onHandleColor="#1E4B72"
                    offHandleColor="#FFB216"
                    height={32}
                    width={60}
                    className="input-switch"
                    id="material-switch"
                    uncheckedHandleIcon={
                        <div
                            style={{
                            display: "flex",
                            justifyContent: "center",
                            alignItems: "center",
                            height: "100%",
                            fontFamily:'Roboto',
                            fontSize: 14,
                            color: "white",}}
                        >
                            B
                        </div>
                    }
                    checkedHandleIcon={
                        <div
                            style={{
                            display: "flex",
                            justifyContent: "center",
                            alignItems: "center",
                            height: "100%",
                            fontFamily:'Roboto',
                            fontSize: 14,
                            color: "white",}}
                        >
                            S
                        </div>
                    }
                        uncheckedIcon={false}
                        checkedIcon={false}
                />
            <button className="button-add"onClick={handleAdd} disabled={ticker.length<1 || vol.length <1 || start_time.length <1 || end_time.length <1 || !datestatus || !ticker_match || !lengthlist || timevalid || LengthListTicker===0}> Add </button>
        </div>
    )
}

export default AddList;