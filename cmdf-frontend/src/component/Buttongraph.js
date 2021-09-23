import React,{useState,useEffect} from 'react'
import './style2.css'
import Modal from 'react-modal'
import Chart from "react-apexcharts";
import { ModalBody } from 'react-bootstrap';
import close from '../source/Vector.svg'
import barchart from '../source/bar-chart1.svg'
import barchartdis from '../source/bar-chartdis.svg'

function Buttongraph({output,list,onSearch}){
    const [show, setShow] = useState(false);
    const handleClose = () => setShow(false);         
    const handleShow = () => setShow(true);
    
    const [data]=useState([]);
    const [ticker]=useState([]);
    const [volume]=useState([]);
    const [starttime]=useState([]);
    const [endtime]=useState([]);
    function mapdiff() {
        output.map((diff)=>{
            data.push(diff.diff)
            ticker.push(diff.ticker)
            volume.push(diff.order_volume)
        }) 
        list.map((time)=>{
          starttime.push(time.start_time)
          endtime.push(time.end_time)
        })
    }
  

  useEffect(() => {
      data.length=0;
      ticker.length=0;
      volume.length=0;
      starttime.length=0;
      endtime.length=0;
    mapdiff();
  }, [output]); 

  const min=Math.min( ...data );
  const graph={
      series:[{
            name : 'Diff(%)',
            type: 'bar',
            data : data
      }],
      options:{
            chart: {
              type: 'bar',
              height: 350 ,
              stacked: true,
              toolbar :{
                show:false
              },
            },
            noData: {
              text: "There's no data",
              align: 'center',
              verticalAlign: 'middle',
              offsetX: 0,
              offsetY: 0
            },
            plotOptions: {
              bar: {
                borderRadius: 4,
                colors: {
                  ranges: [{
                    from: 0,
                    to: 100,
                    color: '#FFB216'
                    }, 
                    {
                    from: -45,
                    to: 0,
                    color: '#1E4B72'
                    }]
                },
                columnWidth: '80%',
              }
            },
            dataLabels: {
              enabled: false,

            },
            
            title: {
              text: 'Diff(%)',
              align: 'left',
              margin: 0,
              offsetX: 0,
              offsetY: 0,
              floating: false,
              style: {
                fontSize:  '18px',
                fontWeight:  '500',
                fontFamily:  'Roboto',
                color:  '#4040404'
              },
            },
            yaxis: {
              labels: {
                show: true,
                style: {
                  colors: '#1E4B72',
                  fontSize: '12px',
                  fontFamily: 'Roboto',
                  fontWeight: 500,
                },
                min:Math.min( ...data ),
                max:Math.max( ...data ),
                tickAmount: 3,
                formatter: function (val) {
                  return val.toFixed(3) + '%'
              }
              }
            },
            tooltip: {
              custom: function({ series, seriesIndex, dataPointIndex, w }) {
                return (
                  '<div class="ticker-block">' +
                    "<div class='grid-ticker'>" +
                      w.globals.labels[dataPointIndex] + 
                    "</div>" 
                    + "<div class='grid-diff'>" +
                      series[seriesIndex][dataPointIndex] + '%'+
                    "</div>" +
                      "<div class='vol'>"+ 'Vol'+ "</div>"+
                    "<div class='item-vol'>"+ volume[dataPointIndex]+ "</div>"+
                    "<div class='item-time'>" + starttime[dataPointIndex] + "  -  " +endtime[dataPointIndex]+" "+ "</div>"+
                  "</div>" 
                  );
                }
              },
            xaxis : {
              type: 'ticker',
              categories: ticker,
              offsetX: 0,
              offsetY: 0,
              labels: {
                show:false
              },
              axisBorder: {
                show: false
              },
              axisTicks: {
                show: false
              }
            }
      }}


    return(
        <div>
            <button  className="button-graph" onClick={handleShow} disabled={output.length === 0}>
              <div className="text-graph"> Diff(%) - Ticker </div>
              {output.length === 0? <img src={barchartdis} className="logo-graph"alt="logo" />:<img src={barchart} className="logo-graph"alt="logo" />}
            </button>
            <Modal
            isOpen={show}
            className="modal-graph"
            overlayClassName="modal-Overlay"
            >
                  <ModalBody>
                  <button className="close-modal" onClick={handleClose}>
                        <img src={close} alt="logo" />
                  </button>
                  <div className="graph">
                    <Chart options={graph.options} series={graph.series} type="bar" height={393} width={560} />
                  </div>
                  </ModalBody>
            </Modal>
        </div>
    );
}
export default Buttongraph;