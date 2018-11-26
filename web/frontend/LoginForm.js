import React, { Component } from 'react';
import './App.css';

import config from './config'

export default class extends Component {
    constructor(props) {
        super(props);
        this.state = {
            email: '',
            password: ''
        };

        this.handleChange = this.handleChange.bind(this);
        this.onLogin = this.onLogin.bind(this)
    }

    async onLogin() {
        try {
            const response = await fetch('/api/login', {
                method: 'POST',
                body: JSON.stringify({
                    username: this.state.email,
                    password: this.state.password
                }),
                headers: {
                    "Content-Type": "application/json"
                }
            });
            // Store token in local storage
            const token = await response.text();
            localStorage.setItem(config.OBI_TOKEN_KEY, token)
        }
        catch(err) {
            // TODO: display error message
        }
    }

    handleChange(event) {
        this.setState({
            [event.target.name]: event.target.value
        })
    }

    render() {
        return (
            <div className="LoginForm">
                <input type="email" placeholder="Email" name="email" onChange={this.handleChange} value={this.state.email} required />
                <input type="password" placeholder="Enter Password" name="password" onChange={this.handleChange} value={this.state.password} required />

                <button type="button" className="loginBtn" onClick={this.onLogin}>Login</button>
                <span className="psw">Forgot <a className="App-link" href="https://www.google.com">password?</a></span>
            </div>
        );
    }
}
