import React, { Component } from 'react';
import './App.css';

export default class extends Component {
    render() {
        return (
            <div className="LoginForm">
                <input type="email" placeholder="Email" name="email" required />
                <input type="password" placeholder="Enter Password" name="psw" required />

                <button type="button" className="loginBtn">Login</button>
                <span className="psw">Forgot <a className="App-link" href="https://www.google.com">password?</a></span>
            </div>
        );
    }
}
