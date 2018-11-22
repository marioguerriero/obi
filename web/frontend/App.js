import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';

class App extends Component {

  async componentDidMount() {
    const response = await fetch('/ping', {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      } 
    })
    const body = await response.text()
    console.log(body)
  }

  render() {
    return (
      <div className="App">
        <header className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <p>
            Edit <code>src/App.js</code> and save to reload.
          </p>
          <a
            className="App-link"
            href="https://reactjs.org"
            target="_blank"
            rel="noopener noreferrer"
          >
            Learn React
          </a>
        </header>
      </div>
    );
  }
}

export default App;
