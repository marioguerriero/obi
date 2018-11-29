import React, { Component } from 'react';
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
            const response = await fetch('/clusters', {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + localStorage.getItem(config.OBI_TOKEN_KEY)
                }
            });
            this.setState({
                jobs: await response.json()
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
        this.fetchJobs();
        const jobs = this.state.jobs.map((job) =>
            <JobItem job={job}/>
        );

        return (
            <div className="ClusterItem">
                <p>{cluster.name}</p>
                {jobs}
            </div>
        );
    }
}
