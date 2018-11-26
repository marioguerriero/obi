import React, { Component } from 'react';
import './App.css';

import JobItem from './JobItem'

export default class extends Component {
    async fetchJobs() {
        const response = await fetch('/clusters', {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + this.props.token
            }
        });
        this.jobs = await response.json();
    }

    componentWillMount() {
        this.fetchJobs()
    }

    componentWillUnmount() {
        this.fetchJobs()
    }

    render() {
        const cluster = this.props.cluster;

        // Fetch cluster's jobs
        this.fetchJobs();
        this.jobs.map((job) =>
            <JobItem job={job}/>
        );

        return (
            <div className="ClusterItem">
                <p>{cluster.name}</p>
            </div>
        );
    }
}
