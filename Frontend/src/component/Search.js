import React from 'react';
import axios from 'axios';
import Autosuggest from 'react-autosuggest';
import './style2.css'

const theme = {
  container: {
    position: 'absolute'
  },
  input: {
    width: 677,
    height: 10,
    margin:'15px 25px 0px',
    padding: '20px 30px ',
    fontWeight: 500,
    fontSize: 18,
    border: '1px solid #aaa',
    borderTopLeftRadius: 100,
    borderTopRightRadius: 100,
    borderBottomLeftRadius: 100,
    borderBottomRightRadius: 100,
  
  },
  inputFocused: {
    outline: 'none'
  },
  inputOpen: {
    
    borderTopLeftRadius: 10,
    borderTopRightRadius: 10,
    borderBottomLeftRadius: 0,
    borderBottomRightRadius: 0,
  },
  suggestionsContainer: {
    position:'absolute'

 
  },
  suggestionsContainerOpen: {
    display: 'block',
    margin:'0px 25px',
    overflow: 'auto',

    maxHeight: 200,
    width: 737,
    border: '1px solid #aaa',
    backgroundColor: '#fff',

    fontWeight: 500,
    fontSize: 18,
    borderBottomLeftRadius: 10,
    borderBottomRightRadius: 10,
    
  },
  suggestionsList: {
    margin: 0,
    padding: 0,
    listStyleType: 'none',
  },
  suggestion: {
    cursor: 'pointer',
    padding: '10px 20px'
  },
  suggestionHighlighted: {
    backgroundColor: '#ddd'
  }
};


// Teach Autosuggest how to calculate suggestions for any given input value.
const getSuggestions = (value,tick) => {
  const inputValue = value.trim().toLowerCase();
  const inputLength = inputValue.length;

  return inputLength === 0 ? [] : tick.filter(lang =>
    lang.ticker.toLowerCase().includes(inputValue) 
  );
};

// When suggestion is clicked, Autosuggest needs to populate the input
// based on the clicked suggestion. Teach Autosuggest how to calculate the
// input value for every given suggestion.
const getSuggestionValue = suggestion => suggestion.ticker;

// Use your imagination to render suggestions.
const renderSuggestion = suggestion => (
  <div className="suggest-input">
    <div className="suggest-ticker">
      {suggestion.ticker} &nbsp;
    </div>
    <div className="suggest-color">
      - {suggestion.side}, {suggestion.volume} , {suggestion.start_time} - {suggestion.end_time}
    </div>
  </div>
);

class Search extends React.Component {
  constructor() {
    super();

    // Autosuggest is a controlled component.
    // This means that you need to provide an input value
    // and an onChange handler that updates this value (see below).
    // Suggestions also need to be provided to the Autosuggest,
    // and they are initially empty because the Autosuggest is closed.
    this.state = {
      value: '',
      suggestions: []
    };
  }

  onChange = (event, { newValue }) => {
    this.setState({
      value: newValue
    });
    //this.props.setNewList(this.state.suggestions)
    //this.props.setOnsearch(true)
    if(newValue===""){
      this.props.setOnsearch(false)
    }
  };

  // Autosuggest will call this function every time you need to update suggestions.
  // You already implemented this logic above, so just use it.
  onSuggestionsFetchRequested = ({ value }) => {
    this.setState({
      suggestions: getSuggestions(value,this.props.clonelist)
    });
    const sug = getSuggestions(value,this.props.clonelist)
    console.log('sug',sug)
    this.props.setNewList(sug)
    this.props.setOnsearch(true)
  };

  // Autosuggest will call this function every time you need to clear suggestions.
  onSuggestionsClearRequested = () => {
    this.setState({
      suggestions: []
    });
    
  };

  onSuggestionSelected = (event, { suggestion }) => {
    event.preventDefault();
    console.log('SUG',suggestion)
    const len=Object.keys(this.props.newlist).length;
    const initlist = this.props.newlist.slice(len,len);
    console.log(initlist)
    const newList = initlist.concat(suggestion);
    this.props.setNewList(newList)
    this.props.setOnsearch(true)
}

  render() {
    const { value, suggestions } = this.state;
    // Autosuggest will pass through all these props to the input.
    const inputProps = {
      placeholder: 'Search Ticker',
      value,
      onChange: this.onChange,
    };
    

    // Finally, render it!
    return (
      <div>
      <Autosuggest
        suggestions={suggestions}
        onSuggestionsFetchRequested={this.onSuggestionsFetchRequested}
        onSuggestionsClearRequested={this.onSuggestionsClearRequested}
        getSuggestionValue={getSuggestionValue}
        renderSuggestion={renderSuggestion}
        onSuggestionSelected={this.onSuggestionSelected}
        inputProps={inputProps}
        theme={theme}
        highlightFirstSuggestion={true}
      />

      </div>
    );
  }
}

export default Search;