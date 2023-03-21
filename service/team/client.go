package team

import (
	"net/http"

	"github.com/onfleet/gonfleet"
	"github.com/onfleet/gonfleet/netw"
	"github.com/onfleet/gonfleet/util"
)

type Client struct {
	apiKey       string
	rlHttpClient *netw.RlHttpClient
	url          string
}

func Plug(apiKey string, rlHttpClient *netw.RlHttpClient, url string) *Client {
	return &Client{
		apiKey:       apiKey,
		rlHttpClient: rlHttpClient,
		url:          url,
	}
}

func (c *Client) List() ([]onfleet.Team, error) {
	teams := []onfleet.Team{}
	err := netw.Call(c.apiKey, c.rlHttpClient, http.MethodGet, c.url, nil, &teams)
	return teams, err
}

func (c *Client) Create(params onfleet.TeamCreateParams) (onfleet.Team, error) {
	team := onfleet.Team{}
	err := netw.Call(c.apiKey, c.rlHttpClient, http.MethodPost, c.url, params, &team)
	return team, err
}

func (c *Client) Update(teamId string, params onfleet.TeamUpdateParams) (onfleet.Team, error) {
	team := onfleet.Team{}
	url := util.UrlAttachPath(c.url, teamId)
	err := netw.Call(c.apiKey, c.rlHttpClient, http.MethodPut, url, params, &team)
	return team, err
}

func (c *Client) Delete(teamId string) error {
	url := util.UrlAttachPath(c.url, teamId)
	err := netw.Call(c.apiKey, c.rlHttpClient, http.MethodDelete, url, nil, nil)
	return err
}
