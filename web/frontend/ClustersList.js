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
import { PanelGroup } from 'react-bootstrap';
import './App.css';

import ClusterItem from './ClusterItem'

import config from './config'
import utils from './utils'

export default class extends Component {
    constructor(props) {
        super(props);
        this.state = {
            clusters: [],
            activeKey: null
        };

        this.fetchClusters = this.fetchClusters.bind(this);
        this.handleSelect = this.handleSelect.bind(this);
    }

    async fetchClusters() {
        try {
            const response = await fetch('/api/clusters', {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + localStorage.getItem(config.OBI_TOKEN_KEY)
                }
            });
            if(response.status !== 200) {
                // Delete authentication token if error is 401
                if(response.status === 401) {
                    utils.clearToken()
                }
                throw Error("Could not load any resource")
            }
            this.setState({
                clusters: await response.json()
            });
        }
        catch (err) {
            this.setState({
                clusters: []
            })
        }
    }

    handleSelect(activeKey) {
        this.setState({ activeKey });
    }

    async componentWillMount() {
        await this.fetchClusters()
    }

    render() {
        // Create clusters list
        let content = <p>No Clusters</p>;
        if(this.state.clusters.length) {
            let i = 1;
            content = this.state.clusters.map(cluster =>
                <ClusterItem eventKey={''+i++} cluster={cluster}/>
            );
        }

        return (
            <div className="ClustersList">
                <PanelGroup
                    accordion
                    id="clusters-group"
                    activeKey={this.state.activeKey}
                    onSelect={this.handleSelect}
                >
                    {content}
                </PanelGroup>
            </div>
        );
    }
}
