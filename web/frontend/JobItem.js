import React, { Component } from 'react';
import './App.css';
import config from "./config";

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
        return (
            <div className="JobItem">
                <ul>
                    <il className="JobItem-id">{this.state.job.id}</il>
                    <il className="JobItem-status">{this.state.job.status}</il>
                    <il className="JobItem-user">{this.state.username}</il>
                    <il className="JobItem-executablepath">{this.state.executablepath}</il>
                    <il className="JobItem-link">
                        <a href={"https://console.cloud.google.com/dataproc/jobs/" +
                                    this.state.job.platformdependentid + "?region=global"}
                           target="_blank" rel="noopener noreferrer">
                            GCP Output</a></il>
                </ul>
            </div>
        );
    }
}
