import React, { Component } from 'react';
import { Panel } from 'react-bootstrap';
import './App.css';

import JobItem from './JobItem'

import config from './config'

export default class extends Component {
    constructor(props) {
        super(props);
        this.state = {
            jobs: []
        };

        this.fetchJobs = this.fetchJobs.bind(this);
    }

    async fetchJobs() {
        try {
            const params = '?' +
                'cluster=' + this.props.cluster.name;
            const response = await fetch('/api/jobs' + params, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + localStorage.getItem(config.OBI_TOKEN_KEY)
                }
            });
            let jobs = await response.json();
            this.setState({
                jobs: Array.isArray(jobs) ? jobs : [ jobs ]
            })
        }
        catch (err) {
            this.setState({
                jobs: null
            })
        }
    }

    async componentWillMount() {
        await this.fetchJobs()
    }

    render() {
        const cluster = this.props.cluster;

        // Fetch cluster's jobs

        const jobs = this.state.jobs.map((job) =>
            <JobItem job={job}/>
        );

        return (
            <Panel eventKey={this.props.eventKey} className="ClusterItem">
                <Panel.Heading>
                    <Panel.Title toggle>
                        <b className="ClusterItem-name">{cluster.name}</b>
                        <span className="ClusterItem-cost">{cluster.cost} $</span>
                    </Panel.Title>
                </Panel.Heading>
                <Panel.Body collapsible>{jobs}</Panel.Body>
            </Panel>
        );
    }
}
