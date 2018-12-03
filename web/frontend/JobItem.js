import React, { Component } from 'react';
import './App.css';
import config from "./config";
import Col from "react-bootstrap/es/Col";

export default class extends Component {
    constructor(props) {
        super(props);

        this.state = {
            job: props.job,
            username: null
        }
    }

    async fetchUsername() {
        try {
            const response = await fetch('/api/user/' + this.state.job.author, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + localStorage.getItem(config.OBI_TOKEN_KEY)
                }
            });
            let user = await response.json();
             this.setState({
                username: user.email
            })
        }
        catch (err) {
            this.setState({
                username: null
            })
        }
    }

    async componentWillMount() {
        await this.fetchUsername()
    }

    render() {
        alert(JSON.stringify(this.state.job))
        return (
            <div className="JobItem">
                <Col md={4} sm={6}>
                    <span className="JobItem-id">{this.state.job.id}</span>
                </Col>
                <Col md={4} sm={6}>
                    <span className="JobItem-status">{this.state.job.status}</span>
                </Col>
                <Col md={4} sm={6}>
                    <span className="JobItem-user">{this.state.username}</span>
                </Col>
                <Col md={6} sm={6}>
                    <span className="JobItem-executablepath">{this.state.executablepath}</span>
                </Col>
                <Col md={6} sm={12}>
                    <span className="JobItem-link">
                        <a href={"https://console.cloud.google.com/dataproc/jobs/" +
                                    this.state.job.platformdependentid + "?region=global"}
                           target="_blank" rel="noopener noreferrer">
                            Job Logs</a></span>
                </Col>
            </div>
        );
    }
}
