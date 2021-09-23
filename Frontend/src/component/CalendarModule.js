import React,{ useState, useEffect } from 'react';
import calendarlogo from '../source/calendar.svg';
import Modal from 'react-modal';
import Calendar from 'react-calendar';
import 'react-calendar/dist/Calendar.css';
import axios from 'axios';
import './style.css';

const CalendarModule = ({ datestate, setDate, setlist, setdatestatus, setlistticker }) =>{
  // Get date ex. 1 2 3 .. 31 , fullyear ex. 2020 2021
  const date = datestate.getDate();
  const year = datestate.getFullYear();
  const selectedDate = datestate.getFullYear() + "-" + (datestate.getMonth() + 1) + "-" + datestate.getDate()

  // For reference month , day
  var listmonth = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"];
  const month = listmonth[datestate.getMonth()];
  var listday = ["01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31"]  
  const newdate = listday[date-1];

  // Initial time
  const oneDay = 1000 * 60 *60 *24
  const currentDayInMilli = new Date(selectedDate).getTime();
  // Get next date
  const nextDayInMilli = currentDayInMilli + oneDay
  const nextDate = new Date(nextDayInMilli).getDate();
  const NextDate = listday[nextDate-1];
  // Get previous date
  const prevDayInMilli = currentDayInMilli - oneDay
  const prevDate = new Date(prevDayInMilli).getDate();
  const PrevDate = listday[prevDate-1];

  // Variable for checking the status of the modal. When selecting a date modal will open.
  const [show, setShow] = useState(false);

  const handleClose = () => 
  {
    setShow(false);         // close Calendar 
    setlist([]);            // clear list when choose date 
    setdatestatus(true);    // if not choose date can't use input 
  }

  const handleShow = () => setShow(true); // Open Calendar
  // When Selecting a date to fetch list of ticker
  useEffect(() => {
    axios(`http://localhost:1111/ticker/${selectedDate}`)
        .then((response) => {
            setlistticker(response.data);
        })
  }, [selectedDate]);       

  return ( 
    <div>
      <Modal isOpen = {show} className = "modal-calendar" contentLabel = "Example Modal" >
          <Calendar
            onChange={setDate}
            onClickDay={handleClose}
            value={datestate}
          />
      </Modal>
      <div className = "calendarBlock">
        <div className = "date-calendar" > {newdate} </div>
        <div className = "dateprev-calendar" > {PrevDate} </div>
        <div className = "datenext-calendar" > {NextDate} </div>
      </div>
      <button className = "calendarMonth-Year" onClick = {handleShow}>
        <img src = {calendarlogo} className = "logo-calendar" alt="logo" />
        <div className = "month-calendar"> {month} </div>
        <div className = "year-calendar"> {year} </div>
      </button>
    </div>
  )
}

export default CalendarModule;