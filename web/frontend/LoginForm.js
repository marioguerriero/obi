import React, { Component } from 'react';
import './App.css';

import { Form, FormGroup, FormControl, Checkbox, Col, Button } from 'react-bootstrap';

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

    async onLogin(event) {
        event.preventDefault();
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
            if(response.status !== 200) {
                throw Error("Invalid Credentials")
            }
            const token = await response.text();
            localStorage.setItem(config.OBI_TOKEN_KEY, token);
            this.props.onLoginSuccess()
        }
        catch(err) {
            alert(err);
            this.props.onLoginFail()
        }
    }

    handleChange(event) {
        this.setState({
            [event.target.name]: event.target.value
        })
    }

    render() {
        return (
            <Form horizontal>
                <FormGroup controlId="formHorizontalEmail">
                    <Col sm={2}>
                        Email
                    </Col>
                    <Col sm={10}>
                        <FormControl type="email" placeholder="Email" name="email" required
                                     onChange={this.handleChange} value={this.state.email} />
                    </Col>
                </FormGroup>

                <FormGroup controlId="formHorizontalPassword">
                    <Col sm={2}>
                        Password
                    </Col>
                    <Col sm={10}>
                        <FormControl type="password" placeholder="Password" name="password" required
                                     onChange={this.handleChange} value={this.state.password} />
                    </Col>
                </FormGroup>

                <FormGroup>
                    <Col sm={10}>
                        <Checkbox>Remember me</Checkbox>
                    </Col>
                </FormGroup>

                <FormGroup>
                    <Col sm={10}>
                        <Button type="submit" onClick={this.onLogin}>Sign in</Button>
                    </Col>
                </FormGroup>
            </Form>
        );
    }
}
