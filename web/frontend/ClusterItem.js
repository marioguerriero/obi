// Copyright 2018 
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.

import React, { Component } from 'react';
import { Panel, Col } from 'react-bootstrap';
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
                        <span className="ClusterItem-cost pull-right">{cluster.cost} $</span>
                    </Panel.Title>
                </Panel.Heading>
                <Panel.Body collapsible>
                    <Col xs={12} md={12}>
                        <b>Jobs list</b>
                    </Col>
                    <Col xs={12} md={12}>
                        {jobs}
                    </Col>
                </Panel.Body>
            </Panel>
        );
    }
}
